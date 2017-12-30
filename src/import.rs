use std::fs::File;
use std::io::Read;
use std::time::Instant;
use std::collections::HashMap;

use rayon::prelude::*;

use errors::StrError;
use database::Database;
use commoncrawl::{GetWetRef, WetRef};

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), StrError> {
        let mut word_count = HashMap::new();
        let now = Instant::now();
        let results = sources
            .into_par_iter()
            .map(|source| {
                let mut count = 0u64;
                let mut file = File::open(source)?;
                let content = &mut Vec::new();
                file.read_to_end(content)?;
                let mut word_count = HashMap::new();
                let mut remaining: &[u8] = content;
                while remaining.len() > 0 {
                    let (blob, rem) = remaining.next_wet_ref();
                    remaining = rem;
                    match blob {
                        WetRef::Conversion {
                            content_lang,
                            content,
                            ..
                        } if content_lang.is_some() =>
                        {
                            let info = content_lang.unwrap();
                            let lang = info.lang();
                            let lang_name = lang.eng_name();
                            // let sample_text = content
                            //     .chars()
                            //     .take(100)
                            //     .collect::<String>()
                            //     .replace("\n", "");
                            let confidence = (info.confidence() * 100.0) as i32;
                            if confidence > 50 && lang_name == "English" {
                                for word in content.split_whitespace() {
                                    if word.chars().all(|c| c.is_alphabetic()) {
                                        *word_count.entry(word.to_lowercase()).or_insert(0u64) += 1;
                                    }
                                }
                                // println!("{} {}: {}", confidence, lang_name, sample_text);
                            }
                        }
                        _ => {}
                    }
                    count += 1;
                }
                Ok((count, word_count))
            })
            .collect::<Vec<_>>();
        let elapsed = now.elapsed();
        let elapsed_time =
            elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0;
        let mut total = 0u64;
        for result in results {
            match result {
                Ok((count, local_word_count)) => {
                    total += count;
                    for (word, count) in local_word_count {
                        *word_count.entry(word).or_insert(0u64) += count;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        let mut word_count_vec = Vec::with_capacity(word_count.len());
        for (word, count) in word_count {
            word_count_vec.push((word, count));
        }
        word_count_vec.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        word_count_vec[0..50]
            .iter()
            .for_each(|val| println!("{:?}", val));
        println!("{} records imported in {}s", total, elapsed_time);
        Ok(())
    }
}
