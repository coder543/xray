#![allow(unused)]

use std::collections::HashMap;
use whatlang::Lang;
use std::path::Path;

pub struct Database {
    pages: HashMap<String, String>,
    by_language: HashMap<Lang, Vec<String>>,
}

pub struct Page {
    pub lang: Lang,
    pub content: String,
}

impl Database {
    pub fn new() -> Database {
        Database {
            pages: HashMap::new(),
            by_language: HashMap::new(),
        }
    }

    pub fn load(location: &Path) -> Database {
        unimplemented!()
    }

    pub fn save(&self, location: &Path) {
        unimplemented!()
    }

    pub fn merge(&mut self, database: Database) {
        for (url, page) in database.pages {
            self.pages.insert(url, page);
        }
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn reserve(&mut self, len: usize) {
        self.pages.reserve(len);
    }

    pub fn insert(&mut self, url: String, page: Page) {
        self.by_language
            .entry(page.lang)
            .or_insert(vec![])
            .push(url.clone());
        self.pages.insert(url, page.content);
    }
}
