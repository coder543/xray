use std::path::PathBuf;

use rayon_hash::HashMap;

mod url_storage;
use storage::url_storage::UrlIndex;

#[derive(Debug)]
pub struct Storage {
    data_dir: PathBuf,
    num_pages: u64,
    url_index: UrlIndex,
}

impl Storage {
    pub fn new<IntoPathBuf: Into<PathBuf>>(data_dir: IntoPathBuf) -> Storage {
        let data_dir = data_dir.into();

        let url_index = UrlIndex::load(&data_dir).unwrap();

        let mut num_pages = 0;
        for entry in &url_index.0 {
            num_pages += entry.num_entries;
        }

        Storage {
            data_dir,
            num_pages,
            url_index,
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
        *self = Storage::new(self.data_dir.clone());
    }

    pub fn get_urls(&self, urls: Vec<u64>) -> Vec<String> {
        self.url_index.get_urls(urls)
    }
}
