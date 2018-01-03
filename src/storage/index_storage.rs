#![allow(unused)]

use errors::StrError;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str;
use std::u64;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rayon::prelude::*;
use rayon_hash::{HashMap, HashSet};
use whatlang::Lang;

use super::JUMP_STRIDE;

#[derive(Clone, Debug)]
pub struct IndexedStore {
    pub file_path: PathBuf,
    pub tag: String,
    pub content_offset: u64,
    pub words: HashSet<String>,
    pub jump_table: Vec<(String, u64)>,
}

impl IndexedStore {
    fn load(file_path: String, tag: String, num_entries: u64) -> Result<IndexedStore, Error> {
        let file_path: PathBuf = file_path.into();
        let mut file = BufReader::new(File::open(&file_path)?);

        // ensure that the index and file agree on how many entries exist
        assert_eq!(num_entries, file.read_u64::<LittleEndian>()?);

        let jump_table_len = file.read_u64::<LittleEndian>()?;

        let mut jump_table = Vec::with_capacity(jump_table_len as usize);
        for _ in 0..jump_table_len {
            let word_len = file.read_u8()? as usize;
            let mut word = vec![0; word_len];
            file.read_exact(&mut word)?;
            jump_table.push((
                String::from_utf8(word).unwrap(),
                file.read_u64::<LittleEndian>()?,
            ));
        }

        let mut words = HashSet::new();

        for _ in 0..num_entries {
            let word_len = file.read_u8()? as usize;
            let mut word = vec![0; word_len];
            file.read_exact(&mut word)?;
            words.insert(String::from_utf8(word).unwrap());
        }

        let content_offset = file.seek(SeekFrom::Current(0))?;

        Ok(IndexedStore {
            file_path,
            tag,
            content_offset,
            words,
            jump_table,
        })
    }

    fn get_word<ReadSeek: Read + Seek>(
        reader: &mut ReadSeek,
        word: String,
    ) -> Result<Option<(String, Vec<u64>)>, Error> {
        let cur_word_len = reader.read_u8()? as usize;
        let mut cur_word_bytes = vec![0; cur_word_len];
        reader.read_exact(&mut cur_word_bytes)?;
        let mut cur_word = String::from_utf8(cur_word_bytes).unwrap();
        let mut cur_set_length = reader.read_u64::<LittleEndian>()?;

        if word < cur_word {
            // move backwards a word, we overstepped and the word isn't here
            reader.seek(SeekFrom::Current(-9 - cur_word.len() as i64))?;
            return Ok(None);
        }

        while word > cur_word {
            reader.seek(SeekFrom::Current(cur_set_length as i64 * 8))?;
            let cur_word_len = reader.read_u8()? as usize;
            let mut cur_word_bytes = vec![0; cur_word_len];
            reader.read_exact(&mut cur_word_bytes)?;
            cur_word = String::from_utf8(cur_word_bytes).unwrap();
            cur_set_length = reader.read_u64::<LittleEndian>()?;
        }

        if word != cur_word {
            // move backwards a word, we overstepped and the word isn't here
            reader.seek(SeekFrom::Current(-9 - cur_word.len() as i64))?;
            return Ok(None);
        }

        let mut word_set = Vec::new();

        for _ in 0..cur_set_length {
            word_set.push(reader.read_u64::<LittleEndian>()?);
        }

        Ok(Some((cur_word, word_set)))
    }

    pub fn get_words(&self, mut words: Vec<String>) -> Result<Vec<(String, Vec<u64>)>, Error> {
        words.sort_unstable();
        let mut file = BufReader::new(File::open(&self.file_path).unwrap());

        file.seek(SeekFrom::Start(self.content_offset))?;

        let mut word_sets = Vec::new();

        let mut offsets = self.jump_table.iter().cloned().peekable();
        let mut next_jump_word = offsets.next().unwrap();
        for word in words {
            while word >= next_jump_word.0 {
                let offset = next_jump_word.1;

                next_jump_word = match offsets.next() {
                    Some(jump_word) => jump_word,
                    None => break,
                };

                // only seek once we are in the right range
                if word < next_jump_word.0 {
                    file.seek(SeekFrom::Start(self.content_offset + offset))?;
                }
            }

            match IndexedStore::get_word(&mut file, word.clone()) {
                Ok(Some((word, set))) => {
                    let _ = word_sets.push((word, set));
                }
                Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(word_sets),
                Err(err) => Err(err)?,
                _ => {}
            }
        }

        Ok(word_sets)
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexedData {
    pub langs: HashMap<String, HashSet<u64>>,
    pub stores: Vec<IndexedStore>,
}

impl IndexedData {
    fn load_index(reader: &mut Read) -> Result<(String, String, u64), Error> {
        let tag_len = reader.read_u8()? as usize;
        let mut tag = vec![0; tag_len];
        reader.read_exact(&mut tag)?;

        let num_entries = reader.read_u64::<LittleEndian>()?;

        let store_path_len = reader.read_u16::<LittleEndian>()? as usize;
        let mut store_path_bytes = vec![0; store_path_len];
        reader.read_exact(&mut store_path_bytes)?;
        let file_path = String::from_utf8(store_path_bytes).unwrap();

        Ok((file_path, String::from_utf8(tag).unwrap(), num_entries))
    }

    pub fn load() -> Result<IndexedData, StrError> {
        let indexed_idx_store_path = "indexed.xraystore";
        let mut indexed_idx_store = BufReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&indexed_idx_store_path)
                .expect(&format!(
                    "Could not open or create URL storage file {}",
                    indexed_idx_store_path,
                )),
        );

        let mut indexed_files = Vec::new();
        loop {
            match IndexedData::load_index(&mut indexed_idx_store) {
                Ok(index) => indexed_files.push(index),
                Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => Err(err)?,
            }
        }

        let maybe_table_entries = indexed_files
            .into_par_iter()
            .map(|(file_path, tag, num_entries)| IndexedStore::load(file_path, tag, num_entries))
            .collect::<Vec<_>>();

        let mut table_entries = Vec::new();
        for entry in maybe_table_entries {
            table_entries.push(entry?);
        }

        let lang_map = {
            let lang_store = table_entries.iter().filter(|x| x.tag == "by_language");

            let mut result = HashMap::new();
            for store in lang_store {
                let temp = store
                    .get_words(vec![
                        "eng".to_string(),
                        "spa".to_string(),
                        "fra".to_string(),
                    ])
                    .unwrap();
                for (lang, set) in temp {
                    result.entry(lang).or_insert_with(HashSet::new).extend(set)
                }
            }

            result
        };

        Ok(IndexedData {
            langs: lang_map,
            stores: table_entries,
        })
    }

    pub fn get_words(&self, tag: &str, mut words: Vec<String>) -> HashMap<String, HashSet<u64>> {
        words.sort_unstable();

        let mut word_map = HashMap::new();
        for store in self.stores.iter().filter(|store| store.tag == tag) {
            let elements = words
                .iter()
                .cloned()
                .filter(|x| store.words.contains(x))
                .collect::<Vec<_>>();
            let elements_len = elements.len();
            let elements_map = store.get_words(elements).unwrap();
            assert_eq!(elements_map.len(), elements_len);

            // if a particular word exist in multiple stores, we want to collate the results
            for (word, set) in elements_map {
                word_map
                    .entry(word)
                    .or_insert_with(HashSet::new)
                    .extend(set);
            }
        }

        word_map
    }
}

fn build_indexed_jump_table(sorted_words: &Vec<(String, Vec<u64>)>) -> Vec<(String, u64)> {
    // ensure that the jump table will always have at least one entry
    let mut jump_table = Vec::new();
    let mut jump_loc = 0u64;
    let mut jump_idx = 0;

    for &(ref word, ref ids) in sorted_words {
        // emit a jump table entry for every JUMP_STRIDE words
        if jump_idx % JUMP_STRIDE == 0 {
            jump_table.push((word.to_string(), jump_loc));
        }

        // 9 byte header per word: 1 byte for word length + 8 bytes for the set length
        jump_loc += word.len() as u64 + ids.len() as u64 * 8 + 9;
        jump_idx += 1;
    }

    jump_table
}

pub fn append_index(
    data_dir: &Path,
    indexed_store_loc: &str,
    tag: &str,
    num_entries: u64,
) -> Result<(), Error> {
    let mut indexed_idx_store = BufWriter::new(OpenOptions::new()
        .append(true)
        .open(data_dir.join("indexed.xraystore"))?);

    // write out the tag for the indexed store in overall index first
    indexed_idx_store.write_u8(tag.len() as u8)?;
    indexed_idx_store.write(tag.as_bytes())?;

    // write out how many words are in this file
    indexed_idx_store.write_u64::<LittleEndian>(num_entries)?;

    // save the file name of this URL store
    indexed_idx_store.write_u16::<LittleEndian>(indexed_store_loc.len() as u16)?;
    indexed_idx_store.write(indexed_store_loc.as_bytes())?;

    Ok(())
}

pub fn store_indexed(
    tag: &str,
    unique: u64,
    data_dir: &Path,
    mut indexed_data: Vec<(String, Vec<u64>)>,
) -> Result<(), StrError> {
    if indexed_data.is_empty() {
        return Ok(());
    }

    indexed_data.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let jump_table = build_indexed_jump_table(&indexed_data);

    let indexed_store_loc = &format!("indexed_{}_{}.xraystore", tag, unique);
    let mut indexed_store = BufWriter::new(File::create(data_dir.join(indexed_store_loc))?);

    if !tag.contains("_tmp") {
        append_index(data_dir, indexed_store_loc, tag, indexed_data.len() as u64)?;
    }

    // write out how many words are in this file
    indexed_store.write_u64::<LittleEndian>(indexed_data.len() as u64)?;

    // write out the number of entries in the jump table
    indexed_store.write_u64::<LittleEndian>(jump_table.len() as u64)?;

    // write out the jump table
    for (word, loc) in jump_table {
        let word = word.as_bytes();
        indexed_store.write_u8(word.len() as u8)?;
        indexed_store.write(word)?;
        indexed_store.write_u64::<LittleEndian>(loc)?;
    }

    // now we need to write out all the words for the HashSet to load initially
    for &(ref word, _) in &indexed_data {
        let word = word.as_bytes();
        assert!(word.len() <= 255);

        // we write out the word length first
        indexed_store.write_u8(word.len() as u8)?;

        // then write out the word
        indexed_store.write(word)?;
    }

    // now we need to write out each word
    for (word, url_ids) in indexed_data {
        let word = word.as_bytes();
        assert!(word.len() <= 255);

        // we write out the word length first
        indexed_store.write_u8(word.len() as u8)?;

        // then write out the word
        indexed_store.write(word)?;

        // and finally store the url_ids
        indexed_store.write_u64::<LittleEndian>(url_ids.len() as u64);
        for id in url_ids {
            indexed_store.write_u64::<LittleEndian>(id);
        }
    }

    Ok(())
}
