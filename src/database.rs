use rayon_hash::HashMap;
use whatlang::Lang;

use errors::StrError;
use helpers::{add_pairs, canonicalize};
use storage::Storage;

#[derive(Clone, Debug)]
pub struct Database {
    storage: Storage,
}

pub struct Page {
    pub lang: Lang,
    pub title: Vec<String>,
    pub words: Vec<String>,
}

impl Database {
    pub fn new(storage: Storage) -> Database {
        Database { storage }
    }

    pub fn insert_url(&mut self, url: String) -> u64 {
        self.storage.insert_url(url)
    }

    pub fn insert(&mut self, url: u64, page: Page) {
        let Page { title, words, lang } = page;

        self.storage.insert_lang(url, lang);

        for title_word in title {
            self.storage.insert_word(url, true, title_word);
        }

        for word in words {
            self.storage.insert_word(url, false, word);
        }
    }

    pub fn persist_urls(&mut self) {
        self.storage.persist_urls();
    }

    pub fn persist(&mut self, unique: Option<u64>) {
        self.storage.persist(unique);
    }

    pub fn optimize(&mut self, chunk_size: usize) -> Result<(), StrError> {
        self.storage.optimize(chunk_size)
    }

    pub fn rebuild_index(&mut self) -> Result<(), StrError> {
        self.storage.rebuild_index()
    }

    pub fn num_stores(&mut self) -> usize {
        self.storage.num_indexed_stores() / 3
    }

    pub fn query(&mut self, words: Vec<String>, lang: Option<Lang>) {
        let mut words_with_pairs = words
            .into_iter()
            .filter_map(|word| canonicalize(&word))
            .collect::<Vec<_>>();

        add_pairs(&mut words_with_pairs);

        let (title_sets, content_sets) = self.storage
            .get_word_sets(lang.unwrap_or(Lang::Eng), words_with_pairs);

        if title_sets.is_empty() && content_sets.is_empty() {
            println!("no matches found");
            return;
        }

        let mut results = HashMap::new();

        for word in title_sets {
            for url in word.1 {
                *results.entry(url).or_insert(0) += word.0.len() * 2;
            }
        }

        for word in content_sets {
            for url in word.1 {
                *results.entry(url).or_insert(0) += word.0.len();
            }
        }

        let mut results = results.into_iter().collect::<Vec<_>>();
        results.sort_by_key(|r| r.1);
        results.reverse();

        let len = results.len();

        let results: Vec<u64> = results.into_iter().take(10).map(|r| r.0).collect();

        let result_map = self.storage.get_urls(results.clone());

        println!("{} results", len);

        results
            .iter()
            .for_each(|url_id| println!("{}", result_map[url_id]));
    }
}
