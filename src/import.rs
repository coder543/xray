use helpers::add_pairs;
use helpers::canonicalize;
use std::fs::File;
use std::fs::read_dir;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::Instant;

use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use whatlang::{detect, Lang};

use commoncrawl::{GetWetRef, WetRef};
use database::Database;
use database::Page;
use errors::StrError;
use helpers::ReadableDuration;

fn load_source(source: PathBuf) -> Result<Vec<(String, Page)>, StrError> {
    let mut raw_pages = Vec::new();

    // shorten peak memory usage time by deallocating `content` after this block
    {
        let is_gzip = source.to_str().unwrap().ends_with(".gz");

        let mut file = File::open(&source)?;
        let content = &mut Vec::new();

        if is_gzip {
            if let Err(_) = MultiGzDecoder::new(BufReader::new(file)).read_to_end(content) {
                return Err(format!(
                    "decoding gzip stream failed for {}",
                    source.display()
                ))?;
            }
        } else {
            file.read_to_end(content)?;
        }

        content.shrink_to_fit();

        let mut remaining: &[u8] = content;
        while !remaining.is_empty() {
            let (blob, rem) = remaining.next_wet_ref();
            remaining = rem;
            if let WetRef::Conversion { url, content, .. } = blob {
                raw_pages.push((url.to_string(), content.to_string()))
            }
        }
    }

    raw_pages.shrink_to_fit();

    let mut pages = raw_pages
        .into_par_iter()
        .filter_map(|(url, content)| {
            let lang = detect(&content)?.lang();
            let title_end = content.find('\n').unwrap_or(0);
            let (mut title, content) = content.split_at(title_end);

            if title.len() > 280 {
                title = ""; // title is invalid
            }

            let mut title = title
                .split_whitespace()
                .filter_map(canonicalize)
                .collect::<Vec<_>>();

            add_pairs(&mut title);

            title.sort_unstable();
            title.dedup();
            title.shrink_to_fit();

            let mut words = content
                .split_whitespace()
                .filter_map(canonicalize)
                .collect::<Vec<_>>();

            if words.len() < 10 {
                return None;
            }

            add_pairs(&mut words);

            words.sort_unstable();
            words.dedup();
            words.shrink_to_fit();

            if lang == Lang::Eng || lang == Lang::Spa || lang == Lang::Fra {
                Some((url, Page { lang, title, words }))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    pages.shrink_to_fit();

    Ok(pages)
}

fn path_to_files(path: String) -> Vec<PathBuf> {
    let path = PathBuf::from(path);
    if path.is_file() {
        return vec![path];
    }

    let mut files = Vec::new();
    if let Ok(dir) = read_dir(&path) {
        for entry in dir {
            if let Ok(entry) = entry {
                let entry = entry.path();
                let file_name = entry.to_str().unwrap();
                if entry.is_file()
                    && (file_name.ends_with(".wet") || file_name.ends_with(".wet.gz"))
                {
                    files.push(entry.to_owned());
                }
            }
        }
    } else {
        panic!("ERROR: invalid path provided! Path was {}", path.display());
    }

    files
}

impl Database {
    pub fn import(&mut self, sources: Vec<String>, chunk_size: usize) -> Result<(), StrError> {
        let now = Instant::now();

        println!("loading source list");
        let sources = sources
            .into_par_iter()
            .flat_map(path_to_files)
            .collect::<Vec<_>>();

        let chunk_offset = self.num_stores();

        sources
            .chunks(chunk_size)
            .enumerate()
            .for_each(|(chunk_num, chunk)| {
                let now = Instant::now();
                let chunk_len = chunk.len();
                println!("loading {} sources", chunk_len);
                let results = chunk
                    .into_par_iter()
                    .cloned()
                    .map(load_source)
                    .collect::<Vec<_>>();

                println!("sources loaded, now importing into database");

                // sequential segment, generate URL IDs then persist the URL database
                let mut results = results
                    .into_iter()
                    .filter_map(|pages| {
                        let pages = match pages {
                            Ok(pages) => pages,
                            Err(err) => {
                                eprintln!("Error: {}", err.0);
                                return None;
                            }
                        };
                        let mut pages = pages
                            .into_iter()
                            .map(|(url, page)| (self.insert_url(url), page))
                            .collect::<Vec<_>>();
                        pages.shrink_to_fit();
                        Some(pages)
                    })
                    .collect::<Vec<_>>();
                results.shrink_to_fit();

                self.persist_urls();

                results.into_iter().enumerate().for_each(|(i, pages)| {
                    println!("processing segment {}/{}", i + 1, chunk_len);

                    for (url, page) in pages {
                        self.insert(url, page)
                    }
                });

                println!("persisting database");
                self.persist(Some((chunk_num + chunk_offset) as u64));

                println!("segments imported in {}", now.elapsed().readable());
            });

        println!("sources imported in {}", now.elapsed().readable());

        Ok(())
    }
}
