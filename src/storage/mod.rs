use std::path::{Path, PathBuf};
use std::time::Instant;
use storage::index_storage::append_index;

use rayon::prelude::*;
use rayon_hash::{HashMap, HashSet};
use whatlang::Lang;

mod url_storage;
use storage::url_storage::UrlIndex;

mod index_storage;
use storage::index_storage::IndexedData;

use errors::StrError;
use helpers::ReadableDuration;

const JUMP_STRIDE: u32 = 1000;

#[derive(Clone, Debug, Default)]
struct ImportProcessing {
    by_language: HashMap<Lang, Vec<u64>>,
    by_word: HashMap<String, Vec<u64>>,
    by_title_word: HashMap<String, Vec<u64>>,
    urls: HashMap<u64, String>,
}

#[derive(Clone, Debug, Default)]
pub struct Storage {
    data_dir: PathBuf,
    num_pages: u64,
    url_index: UrlIndex,
    indexed_data: IndexedData,
    import_processing: ImportProcessing,
}

impl Storage {
    pub fn new<IntoPathBuf: Into<PathBuf>>(data_dir: IntoPathBuf, load_indices: bool) -> Storage {
        use std::env::set_current_dir;
        let data_dir = ::std::fs::canonicalize(data_dir.into()).unwrap();
        set_current_dir(&data_dir).unwrap();

        if !load_indices {
            return Storage {
                data_dir,
                ..Default::default()
            };
        }

        let now = Instant::now();

        let url_index = UrlIndex::load().unwrap();
        let indexed_data = IndexedData::load().unwrap();

        let mut num_pages = 0;
        for entry in &url_index.0 {
            num_pages += entry.num_entries;
        }

        println!("loaded stored indices in {}", now.elapsed().readable());

        Storage {
            data_dir,
            num_pages,
            url_index,
            indexed_data,
            import_processing: Default::default(),
        }
    }

    pub fn insert_url(&mut self, url: String) -> u64 {
        let id = self.num_pages;
        self.import_processing.urls.insert(id, url);

        self.num_pages += 1;

        id
    }

    pub fn insert_lang(&mut self, url_id: u64, lang: Lang) {
        self.import_processing
            .by_language
            .entry(lang)
            .or_insert_with(Vec::new)
            .push(url_id);
    }

    pub fn insert_word(&mut self, url_id: u64, in_title: bool, word: String) {
        let set = if in_title {
            &mut self.import_processing.by_title_word
        } else {
            &mut self.import_processing.by_word
        };

        set.entry(word).or_insert_with(Vec::new).push(url_id);
    }

    pub fn next_unique(&self, tag: &str) -> u64 {
        self.indexed_data
            .stores
            .iter()
            .filter(|store| store.tag == tag)
            .count() as u64
    }

    pub fn persist(&mut self, unique: Option<u64>) {
        use std::mem::replace;

        self.persist_urls();

        let by_language = replace(&mut self.import_processing.by_language, HashMap::new())
            .into_iter()
            .map(|(lang, set)| (lang.code().to_string(), set))
            .collect();
        self.persist_indexed("by_language", unique, by_language);

        let by_title_word = replace(&mut self.import_processing.by_title_word, HashMap::new())
            .into_iter()
            .collect();
        self.persist_indexed("by_title_word", unique, by_title_word);

        let by_word = replace(&mut self.import_processing.by_word, HashMap::new())
            .into_iter()
            .collect();
        self.persist_indexed("by_word", unique, by_word);
    }

    pub fn persist_urls(&mut self) {
        url_storage::store_urls(&self.import_processing.urls).unwrap();
        self.import_processing.urls = HashMap::new();
    }

    pub fn persist_indexed(
        &self,
        tag: &str,
        unique: Option<u64>,
        indexed_data: Vec<(String, Vec<u64>)>,
    ) {
        let unique = unique.unwrap_or_else(|| self.next_unique(tag));
        index_storage::store_indexed(
            tag,
            unique,
            indexed_data
                .into_iter()
                .map(|(word, data)| (word, data.into_iter().collect()))
                .collect(),
        ).unwrap();
    }

    pub fn num_indexed_stores(&mut self) -> usize {
        self.indexed_data.stores.len()
    }

    #[allow(unused)]
    pub fn reload(&mut self) {
        *self = Storage::new(self.data_dir.clone(), true);
    }

    pub fn optimize_tag(&mut self, tag: &str, chunk_size: usize) -> Result<(), StrError> {
        println!("optimizing {}", tag);
        let stores = self.indexed_data
            .stores
            .iter()
            .filter(|store| store.tag == tag)
            .collect::<Vec<_>>();

        let words = stores.iter().map(|store| store.num_entries).max().unwrap();

        (0..words)
            .collect::<Vec<_>>()
            .par_chunks(chunk_size)
            .enumerate()
            .for_each(|(chunk_num, chunk)| {
                println!("stores.get_words - {}", chunk_num);
                let store_data = stores
                    .par_iter()
                    .map(|store| {
                        let map = match store.get_subset_of_words(chunk[0], chunk.len()) {
                            Ok(map) => map,
                            Err(err) => {
                                panic!("store: {}, err: {:?}", store.file_path.display(), err);
                            }
                        };
                        map
                    })
                    .collect::<Vec<_>>();
                println!("aggregating into new_data - {}", chunk_num);

                let mut all_words = store_data
                    .iter()
                    .flat_map(|data| data.iter().map(|&(ref word, _)| word.clone()))
                    .collect::<Vec<_>>();

                all_words.par_sort_unstable();
                all_words.dedup();

                let mut new_data: Vec<(String, Vec<u64>)> = all_words
                    .into_iter()
                    .map(|word| (word, Vec::new()))
                    .collect();

                for data in store_data {
                    for (word, mut set) in data {
                        match new_data.binary_search_by(|&(ref probe, _)| probe.cmp(&word)) {
                            Ok(idx) => new_data[idx].1.append(&mut set),
                            Err(_) => panic!("invalid word!"),
                        };
                    }
                }

                println!("persisting data to disk - {}", chunk_num);
                index_storage::store_indexed(
                    &(tag.to_string() + "_tmp"),
                    chunk_num as u64,
                    new_data,
                ).unwrap();
            });

        Ok(())
    }

    pub fn rebuild_index(&mut self) -> Result<(), StrError> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::fs::{canonicalize, read_dir, rename, File};

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
                        let mut tag = new_name
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

        let index = traverse(&self.data_dir)?;

        File::create("indexed.xraystore")?;
        for (index_path, num_entries, tag) in index {
            append_index(
                &index_path.into_os_string().into_string().unwrap(),
                &tag,
                num_entries,
            )?;
        }

        Ok(())
    }

    pub fn optimize(&mut self, chunk_size: usize) -> Result<(), StrError> {
        use std::fs::remove_file;

        for tag in &["by_word", "by_title_word", "by_language"] {
            self.optimize_tag(tag, chunk_size)?;
        }

        for store in self.indexed_data.stores.drain(..) {
            remove_file(store.file_path)?;
        }
        remove_file(self.data_dir.join("indexed.xraystore"))?;

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
