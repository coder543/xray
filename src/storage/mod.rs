use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use rayon_hash::HashMap;

mod url_storage;

#[derive(Debug)]
pub struct Storage {
    data_dir: PathBuf,
    num_pages: u64,
    url_jump_table: Vec<u64>,
}

impl Storage {
    pub fn new<IntoPathBuf: Into<PathBuf>>(data_dir: IntoPathBuf) -> Storage {
        let data_dir = data_dir.into();

        let url_jump_table = url_storage::load_jump_tables(&data_dir);

        Storage {
            data_dir,
            num_pages: 0,
            url_jump_table: Vec::new(),
        }
    }

    pub fn get_num_pages(&self) -> u64 {
        self.num_pages
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.num_pages;
        self.num_pages += 1;

        id
    }

    pub fn store_urls(&mut self, urls: &HashMap<u64, String>) {
        url_storage::store_urls(&self.data_dir, urls).unwrap();
    }
}
