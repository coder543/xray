use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::Read;
use std::time::Instant;

use rayon::prelude::*;
use whatlang::detect;

use database::Page;
use errors::StrError;
use database::Database;
use commoncrawl::{GetWetRef, WetRef};
use helpers::ReadableDuration;

fn load_source(source: PathBuf) -> Result<Vec<(String, Page)>, StrError> {
    let mut raw_pages = Vec::new();

    // shorten peak memory usage time by deallocating `content` after this block
    {
        let mut file = File::open(source)?;
        let content = &mut Vec::new();
        file.read_to_end(content)?;

        let mut remaining: &[u8] = content;
        while remaining.len() > 0 {
            let (blob, rem) = remaining.next_wet_ref();
            remaining = rem;
            match blob {
                WetRef::Conversion { url, content, .. } => {
                    raw_pages.push((url.to_string(), content.to_string()))
                }
                _ => {}
            }
        }
    }

    let pages = raw_pages
        .into_par_iter()
        .filter_map(|(url, content)| {
            Some((
                url,
                Page {
                    lang: detect(&content)?.lang(),
                    content,
                },
            ))
        })
        .collect::<Vec<_>>();

    Ok(pages)
}

fn path_to_files(path: String) -> Vec<PathBuf> {
    let path = Path::new(&path);
    if path.is_file() {
        return vec![path.to_owned()];
    }

    let mut files = Vec::new();
    match read_dir(path) {
        Ok(dir) => for maybe_entry in dir {
            for entry in maybe_entry {
                let entry = entry.path();
                if entry.is_file() && entry.extension().map_or(false, |ext| ext == "wet") {
                    files.push(entry.to_owned());
                }
            }
        },
        _ => {}
    }

    files
}

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), StrError> {
        let starting_page_count = self.len();

        let now = Instant::now();

        let results = sources
            .into_par_iter()
            .flat_map(path_to_files)
            .into_par_iter()
            .map(load_source)
            .collect::<Vec<_>>();

        let elapsed = now.elapsed().readable();

        for result in results {
            let pages = result?;
            self.reserve(pages.len());
            for (url, page) in pages {
                self.insert(url, page)
            }
        }

        println!(
            "{} pages imported in {}",
            self.len() - starting_page_count,
            elapsed
        );

        Ok(())
    }
}
