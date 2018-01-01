use rayon_hash::HashSet;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct IndexedStore {
    pub file_path: PathBuf,
    pub words: HashSet<String>,
    pub jump_table: Vec<u64>,
}
