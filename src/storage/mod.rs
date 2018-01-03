use std::path::{Path, PathBuf};
use storage::index_storage::append_index;

use rayon::prelude::*;
use rayon_hash::{HashMap, HashSet};
use whatlang::Lang;

mod url_storage;
use errors::StrError;
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

    pub fn next_unique(&self, tag: &str) -> u64 {
        self.indexed_data
            .stores
            .iter()
            .filter(|store| store.tag == tag)
            .count() as u64
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
    }

    pub fn persist_urls(&mut self) {
        url_storage::store_urls(&self.data_dir, &self.import_processing.urls).unwrap();
    }

    pub fn persist_indexed(&self, tag: &str, indexed_data: Vec<(String, HashSet<u64>)>) {
        index_storage::store_indexed(tag, self.next_unique(tag), &self.data_dir, indexed_data)
            .unwrap();
    }

    #[allow(unused)]
    pub fn reload(&mut self) {
        *self = Storage::new(self.data_dir.clone());
    }

    pub fn optimize_tag(&mut self, tag: &str) -> Result<(), StrError> {
        let stores = self.indexed_data
            .stores
            .iter()
            .filter(|store| store.tag == tag)
            .collect::<Vec<_>>();

        let mut words = stores
            .iter()
            .flat_map(|store| store.words.iter())
            .map(|word| word.to_string())
            .collect::<Vec<_>>();

        words.sort_unstable();
        words.dedup();

        println!("total words: {}", words.len());

        words
            .par_chunks(5_000_000)
            .enumerate()
            .for_each(|(chunk_num, chunk)| {
                let mut new_data = HashMap::new();
                let store_data = stores
                    .par_iter()
                    .map(|store| {
                        println!("  - {} store.get_words", chunk_num);
                        let map = match store.get_words(chunk.to_vec()) {
                            Ok(map) => map,
                            Err(err) => {
                                panic!("store: {}, err: {:?}", store.file_path.display(), err);
                            }
                        };
                        map
                    })
                    .collect::<Vec<_>>();
                println!("  - {} for loop", chunk_num);
                for data in store_data {
                    for (word, set) in data {
                        new_data
                            .entry(word)
                            .or_insert_with(HashSet::new)
                            .extend(set);
                    }
                }
                println!("{} persist chunk", chunk_num);
                let new_data = new_data.into_iter().collect();
                index_storage::store_indexed(
                    &(tag.to_string() + "_tmp"),
                    chunk_num as u64,
                    &self.data_dir,
                    new_data,
                ).unwrap();
            });

        Ok(())
    }

    pub fn rebuild_index(&mut self) -> Result<(), StrError> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::fs::{canonicalize, read_dir, remove_file, rename, File};

        fn traverse(path: &Path) -> Result<Vec<(PathBuf, u64, String)>, StrError> {
            let mut results = Vec::new();
            for entry in read_dir(path)? {
                let entry = entry?;
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    results.extend(traverse(&entry_path)?);
                } else if entry_path.is_file() {
                    let file_name = entry_path
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string();
                    // did we find an indexed file?
                    if file_name.starts_with("indexed_") && file_name.ends_with(".xraystore") {
                        let new_name = file_name.replace("_tmp", "");
                        let mut tag = file_name
                            .replace("indexed_", "")
                            .chars()
                            .take_while(|&x| !x.is_numeric())
                            .collect::<String>();
                        tag.pop(); // remove trailing underscore
                        let new_path = path.join(new_name);
                        rename(entry_path, &new_path)?;
                        let mut file = File::open(&new_path)?;
                        let num_entries = file.read_u64::<LittleEndian>()?;

                        results.push((canonicalize(new_path)?, num_entries, tag));
                    }
                }
            }

            Ok(results)
        }

        for store in self.indexed_data.stores.drain(..) {
            remove_file(store.file_path)?;
        }
        remove_file(self.data_dir.join("indexed.xraystore"))?;

        let index = traverse(&self.data_dir)?;

        File::create(self.data_dir.join("indexed.xraystore"))?;
        for (index_path, num_entries, tag) in index {
            append_index(
                &self.data_dir,
                &index_path.into_os_string().into_string().unwrap(),
                &tag,
                num_entries,
            )?;
        }

        Ok(())
    }

    pub fn optimize(&mut self) -> Result<(), StrError> {
        for tag in &["by_word", "by_title_word", "by_language"] {
            self.optimize_tag(tag)?;
        }

        self.rebuild_index()?;

        Ok(())
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
