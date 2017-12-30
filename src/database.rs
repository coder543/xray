#![allow(unused)]

use helpers::canonicalize;
use rayon_hash::HashMap;
use rayon_hash::HashSet;
use std::default::Default;
use std::path::Path;

use rayon::prelude::*;
use whatlang::Lang;

use helpers::canonical_eq;

#[derive(Clone, Default)]
pub struct Database {
    urls: Vec<String>,
    by_language: HashMap<Lang, HashSet<usize>>,
    by_word: HashMap<String, HashSet<usize>>,
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

    fn index_words(&mut self, url: usize, page: &Page) -> bool {
        let mut words = page.content.split_whitespace().collect::<Vec<_>>();
        words.par_sort_unstable();
        words.dedup_by(|&mut a, &mut b| canonical_eq(a, b));

        let words = words
            .into_iter()
            .filter_map(canonicalize)
            .collect::<Vec<_>>();

        if words.len() < 10 {
            return false;
        }

        for word in words.into_iter() {
            self.by_word
                .entry(word)
                .or_insert_with(|| HashSet::new())
                .insert(url);
        }

        true
    }

    pub fn insert(&mut self, url: String, page: Page) {
        let uid = self.urls.len();
        self.urls.push(url);

        // if the page doesn't contain at least 10 words,
        // then we don't care about it.
        if !self.index_words(uid, &page) {
            self.urls.pop();
            return;
        }

        self.by_language
            .entry(page.lang)
            .or_insert_with(|| HashSet::new())
            .insert(uid);
    }

    pub fn shrink(&mut self) {
        self.urls.shrink_to_fit();
        self.by_language.shrink_to_fit();
        self.by_word.shrink_to_fit();

        self.by_language
            .iter_mut()
            .for_each(|(_lang, hashset)| hashset.shrink_to_fit());

        self.by_word
            .iter_mut()
            .for_each(|(_lang, hashset)| hashset.shrink_to_fit());
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
            .map(|&uid| self.urls.get(uid).unwrap())
            .collect::<Vec<_>>();

        println!("{} results", results.len());

        results.iter().take(10).for_each(|url| println!("{}", url));
    }
}
