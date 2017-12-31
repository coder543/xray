use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use byteorder::{LittleEndian, WriteBytesExt};
use rayon::prelude::*;
use rayon_hash::HashMap;

#[derive(Debug)]
pub struct Storage {
    url_store: File,
    num_pages: u64,
}

impl Storage {
    pub fn new<IntoPathBuf: Into<PathBuf>>(path: IntoPathBuf) -> Storage {
        let path = path.into();
        let url_store_path = path.join("urls.xraystore");
        let url_store = File::create(&url_store_path).expect(&format!(
            "Could not create URL storage file {}",
            url_store_path.display(),
        ));

        Storage {
            url_store,
            num_pages: 0,
        }
    }

    pub fn get_num_pages(&self) -> u64 {
        self.num_pages
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.num_pages;
        self.num_pages += 1;

        id
    }

    pub fn store_urls(&mut self, urls: &HashMap<u64, String>) {
        let mut url_store = BufWriter::new(&mut self.url_store);
        let mut sortable_urls = urls.iter().collect::<Vec<_>>();
        sortable_urls.par_sort_unstable_by_key(|v| v.0);
        for (&uid, url) in sortable_urls {
            url_store.write_u64::<LittleEndian>(uid).unwrap();
            url_store.write(url.as_bytes()).unwrap();
            url_store.write(&[b'\n']).unwrap();
        }
        //
    }
}
