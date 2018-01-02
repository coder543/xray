use std::path::PathBuf;

use rayon_hash::{HashMap, HashSet};

mod url_storage;
use storage::url_storage::UrlIndex;

mod index_storage;
use storage::index_storage::IndexedStore;

const JUMP_STRIDE: u32 = 1000;

#[derive(Debug)]
pub struct Storage {
    data_dir: PathBuf,
    num_pages: u64,
    url_index: UrlIndex,
    urls: HashMap<u64, String>,
}

impl Storage {
    pub fn new<IntoPathBuf: Into<PathBuf>>(data_dir: IntoPathBuf) -> Storage {
        let data_dir = data_dir.into();

        let url_index = UrlIndex::load(&data_dir).unwrap();
        let _indexed_index = IndexedStore::load(&data_dir).unwrap();

        let mut num_pages = 0;
        for entry in &url_index.0 {
            num_pages += entry.num_entries;
        }

        Storage {
            data_dir,
            num_pages,
            url_index,
            urls: HashMap::new(),
        }
    }

    pub fn get_num_pages(&self) -> u64 {
        self.num_pages
    }

    pub fn next_url_id(&mut self, url: String) -> u64 {
        let id = self.num_pages;
        self.urls.insert(id, url);

        self.num_pages += 1;

        id
    }

    pub fn persist_urls(&mut self) {
        url_storage::store_urls(&self.data_dir, &self.urls).unwrap();
    }

    pub fn persist_indexed(&mut self, tag: u64, indexed_data: Vec<(String, HashSet<u64>)>) {
        index_storage::store_indexed(tag, &self.data_dir, indexed_data).unwrap();
    }

    pub fn reload(&mut self) {
        *self = Storage::new(self.data_dir.clone());
    }

    pub fn get_urls(&self, urls: Vec<u64>) -> Vec<String> {
        self.url_index.get_urls(urls)
    }
}
