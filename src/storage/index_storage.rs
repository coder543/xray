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

use super::JUMP_STRIDE;

#[derive(Clone, Debug)]
pub struct IndexedStore {
    pub file_path: PathBuf,
    pub words: HashSet<String>,
    pub jump_table: Vec<u64>,
}

impl IndexedStore {
    pub fn load(data_dir: &Path) -> Result<(), StrError> {
        let indexed_idx_store_path = data_dir.join("indexed.xraystore");
        let mut indexed_idx_store = BufReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&indexed_idx_store_path)
                .expect(&format!(
                    "Could not open or create URL storage file {}",
                    indexed_idx_store_path.display(),
                )),
        );

        Ok(())
    }
}

fn build_indexed_jump_table(sorted_urls: &Vec<(String, HashSet<u64>)>) -> Vec<(String, u64)> {
    let mut jump_table = Vec::new();
    let mut jump_loc = 0u64;
    let mut jump_idx = 0;

    for &(ref word, ref ids) in sorted_urls {
        // emit a jump table entry for every JUMP_STRIDE words
        if jump_idx % JUMP_STRIDE == 0 && jump_idx != 0 {
            jump_table.push((word.to_string(), jump_loc));
        }

        // 9 byte header per word: 1 byte for word length + 8 bytes for the set length
        jump_loc += word.len() as u64 + ids.len() as u64 * 8 + 9;
        jump_idx += 1;
    }

    jump_table
}

pub fn store_indexed(
    tag: u64,
    data_dir: &Path,
    mut indexed_data: Vec<(String, HashSet<u64>)>,
) -> Result<(), StrError> {
    if indexed_data.is_empty() {
        return Ok(());
    }

    indexed_data.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let jump_table = build_indexed_jump_table(&indexed_data);

    let indexed_store_loc = &format!("indexed_{}.xraystore", tag);
    let mut indexed_store = BufWriter::new(File::create(data_dir.join(indexed_store_loc))?);

    let mut indexed_idx_store = BufWriter::new(OpenOptions::new()
        .append(true)
        .open(data_dir.join("indexed.xraystore"))?);

    // write out the tag for the indexed store in overall index first
    indexed_idx_store.write_u64::<LittleEndian>(tag)?;

    // write out how many words are in this file
    indexed_idx_store.write_u64::<LittleEndian>(indexed_data.len() as u64)?;

    // save the file name of this URL store
    indexed_idx_store.write_u16::<LittleEndian>(indexed_store_loc.len() as u16)?;
    indexed_idx_store.write(indexed_store_loc.as_bytes())?;

    // write out the number of entries in the jump table
    indexed_store.write_u64::<LittleEndian>(jump_table.len() as u64)?;

    // write out the stride length of the jump table
    indexed_store.write_u32::<LittleEndian>(JUMP_STRIDE)?;

    // write out the jump table
    for (word, loc) in jump_table {
        let word = word.as_bytes();
        indexed_store.write_u8(word.len() as u8)?;
        indexed_store.write(word)?;
        indexed_store.write_u64::<LittleEndian>(loc)?;
    }

    // now we need to write out each URL
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
