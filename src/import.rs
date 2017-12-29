use std::fs::File;
use std::io::Read;

use database::Database;
use commoncrawl::GetWetRef;

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), String> {
        for source in sources {
            let mut file = File::open(source).unwrap();
            let content = &mut Vec::new();
            file.read_to_end(content).unwrap();
            let mut remaining: &[u8] = content;
            while remaining.len() > 0 {
                let (_blob, rem) = remaining.next_wet_ref();
                remaining = rem;
            }
        }
        Ok(())
    }
}