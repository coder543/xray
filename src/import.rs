use std::fs::File;
use std::io::Read;
use std::time::Instant;

use rayon::prelude::*;

use errors::StrError;
use database::Database;
use commoncrawl::{WetRef, GetWetRef};

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), StrError> {
        let now = Instant::now();
        let results = sources.into_par_iter().map(|source| {
            let mut count = 0u64;
            let mut file = File::open(source)?;
            let content = &mut Vec::new();
            file.read_to_end(content)?;
            let mut remaining: &[u8] = content;
            while remaining.len() > 0 {
                let (blob, rem) = remaining.next_wet_ref();
                remaining = rem;
                // match blob {
                //     WetRef::WarcInfo { .. } => println!("warcinfo"),
                //     WetRef::Conversion { .. } => println!("conversion"),
                // }
                count += 1;
            }
            Ok(count)
        }).collect::<Vec<_>>();
        let elapsed = now.elapsed();
        let elapsed_time = elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0;
        let mut total = 0u64;
        for result in results {
            match result {
                Ok(count) => total += count,
                Err(error) => return Err(error),
            }
        }
        print!("{} records imported in {}s", total, elapsed_time);
        Ok(())
    }
}