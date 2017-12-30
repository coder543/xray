#![allow(unused)]

use std::collections::HashSet;
use std::collections::HashMap;
use std::path::Path;
use std::default::Default;

use whatlang::Lang;

#[derive(Clone, Default)]
pub struct Database {
    next_uid: u64,
    urls: HashMap<u64, String>,
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
        self.urls.insert(uid, url);
        self.index_words(uid, &page);
        self.by_language
            .entry(page.lang)
            .or_insert(HashSet::new())
            .insert(uid);
    }

    pub fn query(&self, words: Vec<String>, lang: Option<Lang>) {
        let mut sets = words
            .into_iter()
            .filter_map(|word| self.by_word.get(&word))
            .map(|x| x.to_owned())
            .collect::<Vec<_>>();

        if let Some(lang) = lang {
            if let Some(lang_set) = self.by_language.get(&lang) {
                sets.push(lang_set.to_owned());
            }
        }

        let mut iter = sets.into_iter();
        let set = iter.next().unwrap();
        let results = iter.fold(set, |set1, set2| &set1 & &set2)
            .iter()
            .map(|uid| self.urls.get(uid).unwrap())
            .collect::<Vec<_>>();

        println!("{} results", results.len());

        results.iter().take(10).for_each(|url| println!("{}", url));
    }
}
