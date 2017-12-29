use std::fs::File;
use std::io::Read;
use std::time::Instant;
use errors::StrError;

use database::Database;
use commoncrawl::{WetRef, GetWetRef};

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), StrError> {
        let mut count = 0;
        let now = Instant::now();
        for source in sources {
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
        }
        let elapsed = now.elapsed();
        let elapsed_time = elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0;
        print!("{} records imported in {}s", count, elapsed_time);
        Ok(())
    }
}