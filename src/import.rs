use database::Page;
use std::fs::File;
use std::io::Read;
use std::time::Instant;

use rayon::prelude::*;

use errors::StrError;
use database::Database;
use commoncrawl::{GetWetRef, WetRef};
use helpers::ReadableDuration;

fn load_source(source: String) -> Result<Vec<(String, Page)>, StrError> {
    let mut file = File::open(source)?;
    let content = &mut Vec::new();
    file.read_to_end(content)?;

    let mut pages = Vec::new();

    let mut remaining: &[u8] = content;
    while remaining.len() > 0 {
        let (blob, rem) = remaining.next_wet_ref();
        remaining = rem;
        match blob {
            WetRef::Conversion {
                url,
                content,
                content_lang,
                ..
            } if content_lang.is_some() =>
            {
                pages.push((
                    url.to_string(),
                    Page {
                        lang: content_lang.unwrap().lang(),
                        content: content.to_string(),
                    },
                ))
            }
            _ => {}
        }
    }

    Ok(pages)
}

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), StrError> {
        let starting_page_count = self.len();

        let now = Instant::now();
        let results = sources.into_par_iter().map(load_source).collect::<Vec<_>>();
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
