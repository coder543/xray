use whatlang::Lang;

use helpers::canonicalize;
use storage::Storage;

#[derive(Debug)]
pub struct Database {
    storage: Storage,
}

pub struct Page {
    pub lang: Lang,
    pub content: String,
}

fn add_pairs(words: &mut Vec<String>) {
    if words.is_empty() {
        return;
    }

    let mut word_pairs = Vec::new();
    let mut last_word = words[0].clone();
    for word in &words[1..] {
        let word_pair = last_word + "|" + word;
        if word_pair.as_bytes().len() <= 255 {
            word_pairs.push(word_pair);
        }
        last_word = word.to_string();
    }

    words.extend(word_pairs);
}

impl Database {
    pub fn new(storage: Storage) -> Database {
        Database { storage }
    }

    pub fn len(&self) -> u64 {
        self.storage.get_num_pages()
    }

    pub fn insert(&mut self, url: String, page: Page) {
        let Page { content, lang } = page;

        let title_end = content.find('\n').unwrap_or(0);
        let (mut title, content) = content.split_at(title_end);

        if title.len() > 250 {
            title = ""; // title is invalid
        }

        let mut title_words = title
            .split_whitespace()
            .filter_map(canonicalize)
            .collect::<Vec<_>>();

        add_pairs(&mut title_words);

        title_words.sort_unstable();
        title_words.dedup();

        let mut words = content
            .split_whitespace()
            .filter_map(canonicalize)
            .collect::<Vec<_>>();

        add_pairs(&mut words);

        words.sort_unstable();
        words.dedup();

        if words.len() < 10 {
            return;
        }

        let url = self.storage.insert_url(url, lang);

        for title_word in title_words {
            self.storage.insert_word(url, true, title_word);
        }

        for word in words {
            self.storage.insert_word(url, false, word);
        }
    }

    pub fn persist(&mut self) {
        self.storage.persist();
    }

    pub fn query(&mut self, words: Vec<String>, lang: Option<Lang>) {
        let mut words_with_pairs = words
            .into_iter()
            .filter_map(|word| canonicalize(&word))
            .collect::<Vec<_>>();

        add_pairs(&mut words_with_pairs);

        let (title_sets, content_sets) = self.storage
            .get_word_sets(lang.unwrap_or(Lang::Eng), words_with_pairs);

        // if sets.is_empty() {
        //     println!("no matches found");
        //     return;
        // }

        let mut iter = content_sets.into_iter();
        let set = iter.next().unwrap();
        let results = iter.fold(set.1, |set1, set2| &set1 & &set2.1)
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let len = results.len();

        let results = self.storage
            .get_urls(results.into_iter().take(10).collect());

        println!("{} results", len);

        results.iter().for_each(|url| println!("{}", url.1));
    }
}
