use errors::StrError;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use byteorder::{LittleEndian, WriteBytesExt};
use rayon::prelude::*;
use rayon_hash::HashMap;

const JUMP_STRIDE: u32 = 1000;

fn load_url_jump_tables(data_dir: &Path) -> Vec<u64> {
    let url_idx_store_path = data_dir.join("urls.xraystore");
    let url_idx_store = OpenOptions::new()
        .read(true)
        .create(true)
        .open(&url_idx_store_path)
        .expect(&format!(
            "Could not open or create URL storage file {}",
            url_idx_store_path.display(),
        ));

    unimplemented!();
}

fn build_url_jump_table(sorted_urls: &Vec<(&u64, &String)>) -> Vec<u64> {
    let mut jump_table = Vec::new();
    let mut jump_loc = 0u64;
    let mut jump_idx = 0;

    for &(_, url) in sorted_urls {
        // emit a jump table entry for every JUMP_STRIDE urls
        if jump_idx % JUMP_STRIDE == 0 {
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
