#![allow(unused)]

use std::collections::HashSet;
use std::collections::HashMap;
use std::path::Path;
use std::default::Default;

use whatlang::Lang;

#[derive(Clone, Default)]
pub struct Database {
    next_uid: u64,
    urls: HashMap<String, u64>,
    by_language: HashMap<Lang, HashSet<u64>>,
    by_word: HashMap<String, HashSet<u64>>,
}

pub struct Page {
    pub lang: Lang,
    pub content: String,
}

impl Database {
    pub fn new() -> Database {
        Database {
            ..Default::default()
        }
    }

    pub fn load(location: &Path) -> Database {
        unimplemented!()
    }

    pub fn save(&self, location: &Path) {
        unimplemented!()
    }

    pub fn len(&self) -> usize {
        self.urls.len()
    }

    pub fn reserve(&mut self, len: usize) {
        self.urls.reserve(len);
    }

    fn index_words(&mut self, url: u64, page: &Page) {
        let mut words = page.content.split_whitespace().collect::<Vec<_>>();
        words.sort();
        words.dedup();

        for word in words {
            if word.len() > 2 {
                self.by_word
                    .entry(word.to_string())
                    .or_insert(HashSet::new())
                    .insert(url);
            }
        }
    }

    pub fn insert(&mut self, url: String, page: Page) {
        let uid = self.next_uid;
        self.next_uid += 1;
        self.urls.insert(url, uid);
        self.index_words(uid, &page);
        self.by_language
            .entry(page.lang)
            .or_insert(HashSet::new())
            .insert(uid);
    }
}
