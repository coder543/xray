use std::path::PathBuf;

use rayon_hash::{HashMap, HashSet};
use whatlang::Lang;

mod url_storage;
use storage::url_storage::UrlIndex;

mod index_storage;
use storage::index_storage::IndexedData;

const JUMP_STRIDE: u32 = 1000;

#[derive(Clone, Debug, Default)]
struct ImportProcessing {
    by_language: HashMap<Lang, HashSet<u64>>,
    by_word: HashMap<String, HashSet<u64>>,
    by_title_word: HashMap<String, HashSet<u64>>,
    urls: HashMap<u64, String>,
}

#[derive(Debug)]
pub struct Storage {
    data_dir: PathBuf,
    num_pages: u64,
    url_index: UrlIndex,
    indexed_data: IndexedData,
    import_processing: ImportProcessing,
}

impl Storage {
    pub fn new<IntoPathBuf: Into<PathBuf>>(data_dir: IntoPathBuf) -> Storage {
        let data_dir = data_dir.into();

        let url_index = UrlIndex::load(&data_dir).unwrap();
        let indexed_data = IndexedData::load(&data_dir).unwrap();

        let mut num_pages = 0;
        for entry in &url_index.0 {
            num_pages += entry.num_entries;
        }

        Storage {
            data_dir,
            num_pages,
            url_index,
            indexed_data,
            import_processing: Default::default(),
        }
    }

    pub fn get_num_pages(&self) -> u64 {
        self.num_pages
    }

    pub fn insert_url(&mut self, url: String, lang: Lang) -> u64 {
        let id = self.num_pages;
        self.import_processing.urls.insert(id, url);

        self.import_processing
            .by_language
            .entry(lang)
            .or_insert_with(HashSet::new)
            .insert(id);

        self.num_pages += 1;

        id
    }

    pub fn insert_word(&mut self, url_id: u64, in_title: bool, word: String) {
        let set = if in_title {
            &mut self.import_processing.by_title_word
        } else {
            &mut self.import_processing.by_word
        };

        set.entry(word).or_insert_with(HashSet::new).insert(url_id);
    }

    pub fn persist(&mut self) {
        use std::mem::replace;

        self.persist_urls();

        let by_language = replace(&mut self.import_processing.by_language, HashMap::new())
            .into_iter()
            .map(|(lang, set)| (lang.code().to_string(), set))
            .collect();
        self.persist_indexed("by_language", by_language);

        let by_title_word = replace(&mut self.import_processing.by_title_word, HashMap::new())
            .into_iter()
            .collect();
        self.persist_indexed("by_title_word", by_title_word);

        let by_word = replace(&mut self.import_processing.by_word, HashMap::new())
            .into_iter()
            .collect();
        self.persist_indexed("by_word", by_word);

        self.reload();
    }

    pub fn persist_urls(&mut self) {
        url_storage::store_urls(&self.data_dir, &self.import_processing.urls).unwrap();
    }

    pub fn persist_indexed(&mut self, tag: &str, indexed_data: Vec<(String, HashSet<u64>)>) {
        index_storage::store_indexed(tag, &self.data_dir, indexed_data).unwrap();
    }

    pub fn reload(&mut self) {
        *self = Storage::new(self.data_dir.clone());
    }

    /// gets the associated HashSets for each word, filtered by language
    /// returns (title_words, content_words)
    pub fn get_word_sets(
        &self,
        lang: Lang,
        words: Vec<String>,
    ) -> (HashMap<String, HashSet<u64>>, HashMap<String, HashSet<u64>>) {
        let lang = self.indexed_data.langs.get(lang.code()).unwrap();

        // get the sets and then filter by the current language

        let content_words = self.indexed_data
            .get_words("by_word", words.clone())
            .into_iter()
            .map(|(word, set)| (word, &set & lang))
            .collect();

        let title_words = self.indexed_data
            .get_words("by_title_word", words)
            .into_iter()
            .map(|(word, set)| (word, &set & lang))
            .collect();

        (title_words, content_words)
    }

    pub fn get_urls(&self, urls: Vec<u64>) -> HashMap<u64, String> {
        self.url_index.get_urls(urls)
    }
}
