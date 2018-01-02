use errors::StrError;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str;
use std::u64;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rayon::prelude::*;
use rayon_hash::HashMap;

use super::JUMP_STRIDE;

#[derive(Clone, Debug)]
pub struct UrlStore {
    pub file_path: PathBuf,
    pub first_index: u64,
    pub num_entries: u64,
    pub jump_stride: u64,
    pub jump_table: Vec<u64>,
}

impl UrlStore {
    fn get_url<ReadSeek: Read + Seek>(
        reader: &mut ReadSeek,
        url_idx: u64,
    ) -> Result<String, Error> {
        let url_len = reader.read_u16::<LittleEndian>()? as i64;
        let cur_idx = reader.read_u64::<LittleEndian>()?;
        assert!(url_idx >= cur_idx);
        let distance = url_idx - cur_idx;

        // special case where we land on exactly the right URL
        if distance == 0 {
            let mut url_bytes = vec![0; url_len as usize];
            reader.read_exact(&mut url_bytes)?;

            return Ok(String::from_utf8(url_bytes).unwrap());
        }

        reader.seek(SeekFrom::Current(url_len))?;
        for _ in 0..(distance - 1) {
            let len = reader.read_u16::<LittleEndian>()? as i64;
            reader.seek(SeekFrom::Current(len + 8))?;
        }

        let url_len = reader.read_u16::<LittleEndian>()? as usize;
        let idx = reader.read_u64::<LittleEndian>()?;

        assert!(idx == url_idx);
        let mut url_bytes = vec![0; url_len];
        reader.read_exact(&mut url_bytes)?;

        Ok(String::from_utf8(url_bytes).unwrap())
    }

    pub fn get_urls(&self, url_idxs: &[u64]) -> Result<Vec<String>, Error> {
        let mut file = BufReader::new(File::open(&self.file_path).unwrap());

        // length of the jump table + len(jump_stride) + len(num_entries)
        let start_offset = self.jump_table.len() as u64 * 8 + 12;
        file.seek(SeekFrom::Start(start_offset))?;

        let mut urls = Vec::new();

        let mut offsets = self.jump_table.iter().cloned().peekable();
        let mut jump_idx = 0;
        for &idx in url_idxs {
            while idx > jump_idx + self.jump_stride {
                // if we exceed the jump table + jump_stride, we have been asked to get a URL
                // that is not in the UrlStore, which shows a logic error.
                let cur_offset = offsets.next().unwrap();
                jump_idx += self.jump_stride;

                // only seek once we are in the right range
                if idx < jump_idx + self.jump_stride {
                    file.seek(SeekFrom::Start(start_offset + cur_offset))?;
                }
            }
            urls.push(UrlStore::get_url(&mut file, idx)?);
        }

        Ok(urls)
    }
}

#[derive(Clone, Debug)]
pub struct UrlIndex(pub Vec<UrlStore>);

impl UrlIndex {
    fn load_jump_table(path: &Path) -> Result<(u64, Vec<u64>), Error> {
        let mut file = BufReader::new(File::open(path)?);

        let num_entries = file.read_u64::<LittleEndian>()?;
        let jump_stride = file.read_u32::<LittleEndian>()?;
        let mut jump_table = Vec::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            jump_table.push(file.read_u64::<LittleEndian>()?);
        }

        Ok((jump_stride as u64, jump_table))
    }

    fn load_index(reader: &mut Read, data_dir: &Path) -> Result<UrlStore, Error> {
        let first_index = reader.read_u64::<LittleEndian>()?;
        let num_entries = reader.read_u64::<LittleEndian>()?;

        let store_path_len = reader.read_u16::<LittleEndian>()? as usize;
        let mut store_path_bytes = vec![0; store_path_len];
        reader.read_exact(&mut store_path_bytes)?;
        let file_path = data_dir.join(str::from_utf8(&store_path_bytes).unwrap());

        let (jump_stride, jump_table) = UrlIndex::load_jump_table(&file_path)
            .expect("failed to load URL jump table. Index must be corrupt.");


        Ok(UrlStore {
            file_path,
            first_index,
            num_entries,
            jump_stride,
            jump_table,
        })
    }

    pub fn load(data_dir: &Path) -> Result<UrlIndex, StrError> {
        let url_idx_store_path = data_dir.join("urls.xraystore");
        let mut url_idx_store = BufReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&url_idx_store_path)
                .expect(&format!(
                    "Could not open or create URL storage file {}",
                    url_idx_store_path.display(),
                )),
        );

        let mut table_entries = Vec::new();
        loop {
            match UrlIndex::load_index(&mut url_idx_store, data_dir) {
                Ok(index) => table_entries.push(index),
                Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => Err(err)?,
            }
        }

        Ok(UrlIndex(table_entries))
    }

    pub fn get_urls(&self, mut ids: Vec<u64>) -> Vec<String> {
        ids.sort_unstable();

        let mut urls = Vec::new();
        for store in &self.0 {
            let elements = ids.iter()
                .cloned()
                .filter(|&x| x >= store.first_index && x < store.first_index + store.num_entries)
                .collect::<Vec<_>>();
            urls.extend(store.get_urls(&elements).unwrap());
        }

        assert!(urls.len() == ids.len());
        urls
    }
}

fn build_url_jump_table(sorted_urls: &Vec<(&u64, &String)>) -> Vec<u64> {
    let mut jump_table = Vec::new();
    let mut jump_loc = 0u64;
    let mut jump_idx = 0;

    for &(_, url) in sorted_urls {
        // emit a jump table entry for every JUMP_STRIDE urls
        if jump_idx % JUMP_STRIDE == 0 && jump_idx != 0 {
            jump_table.push(jump_loc);
        }

        // 10 byte header per URL: 8 bytes for the UID, 2 bytes for URL length
        jump_loc += url.len() as u64 + 10;
        jump_idx += 1;
    }

    jump_table
}

pub fn store_urls(data_dir: &Path, urls: &HashMap<u64, String>) -> Result<(), StrError> {
    let mut sortable_urls = urls.iter().collect::<Vec<_>>();
    sortable_urls.par_sort_unstable_by_key(|v| v.0);
    if sortable_urls.is_empty() {
        return Ok(());
    }

    let jump_table = build_url_jump_table(&sortable_urls);

    let start_idx = *sortable_urls[0].0 as u64;
    let url_store_loc = &format!("urls_{}.xraystore", start_idx);
    let mut url_store = BufWriter::new(File::create(data_dir.join(url_store_loc))?);

    let mut url_idx_store = BufWriter::new(OpenOptions::new()
        .append(true)
        .open(data_dir.join("urls.xraystore"))?);

    // write out the starting index for the URLs in this file first
    url_idx_store.write_u64::<LittleEndian>(start_idx)?;

    // write out how many URLs are in this file
    url_idx_store.write_u64::<LittleEndian>(sortable_urls.len() as u64)?;

    // save the file name of this URL store
    url_idx_store.write_u16::<LittleEndian>(url_store_loc.len() as u16)?;
    url_idx_store.write(url_store_loc.as_bytes())?;

    // write out the number of entries in the jump table
    url_store.write_u64::<LittleEndian>(jump_table.len() as u64)?;

    // write out the stride length of the jump table
    url_store.write_u32::<LittleEndian>(JUMP_STRIDE)?;

    // write out the jump table
    for entry in jump_table {
        url_store.write_u64::<LittleEndian>(entry)?;
    }

    // now we need to write out each URL
    for (&uid, url) in sortable_urls {
        let url = url.as_bytes();

        // we write out the URL length first to make it easier to skip through the table
        url_store.write_u16::<LittleEndian>(url.len() as u16)?;

        // then save the unique ID for this URL
        url_store.write_u64::<LittleEndian>(uid)?;

        // and finally store the URL itself
        url_store.write(url)?;
    }

    Ok(())
}
