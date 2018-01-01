use errors::StrError;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::str;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rayon::prelude::*;
use rayon_hash::HashMap;

const JUMP_STRIDE: u32 = 1000;

#[derive(Clone, Debug)]
pub struct UrlIndex {
    pub file_path: PathBuf,
    pub first_index: u64,
    pub num_entries: u64,
    pub jump_stride: u32,
    pub jump_table: Vec<u64>,
}

pub fn load_url_jump_table(path: &Path) -> Result<(u32, Vec<u64>), Error> {
    let mut file = BufReader::new(File::open(path)?);

    let num_entries = file.read_u64::<LittleEndian>()?;
    let jump_stride = file.read_u32::<LittleEndian>()?;
    let mut jump_table = Vec::with_capacity(num_entries as usize);
    for _ in 0..num_entries {
        jump_table.push(file.read_u64::<LittleEndian>()?);
    }

    Ok((jump_stride, jump_table))
}

pub fn load_url_index(reader: &mut Read, data_dir: &Path) -> Result<UrlIndex, Error> {
    let first_index = reader.read_u64::<LittleEndian>()?;
    let num_entries = reader.read_u64::<LittleEndian>()?;

    let url_len = reader.read_u16::<LittleEndian>()? as usize;
    let mut url_bytes = vec![0; url_len];
    reader.read_exact(&mut url_bytes)?;
    let file_path = data_dir.join(str::from_utf8(&url_bytes).unwrap());

    let (jump_stride, jump_table) = load_url_jump_table(&file_path)
        .expect("failed to load URL jump table. Index must be corrupt.");

    println!("jump_table: {:?}", jump_table);

    Ok(UrlIndex {
        file_path,
        first_index,
        num_entries,
        jump_stride,
        jump_table,
    })
}

pub fn load_url_indices(data_dir: &Path) -> Result<Vec<UrlIndex>, StrError> {
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
        match load_url_index(&mut url_idx_store, data_dir) {
            Ok(index) => table_entries.push(index),
            Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => Err(err)?,
        }
    }

    Ok(table_entries)
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
