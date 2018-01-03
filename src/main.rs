extern crate brotli;
extern crate byteorder;
extern crate flate2;
#[macro_use]
extern crate nom;
extern crate rayon;
extern crate rayon_hash;
extern crate rustyline;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
extern crate whatlang;

use structopt::StructOpt;

use std::process::exit;

mod errors;
mod helpers;

mod commoncrawl;
mod database;
mod storage;

mod interactive;
mod search;
mod import;
mod stats;

use database::Database;
use storage::Storage;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "xray")]
/// xray is a primitive search engine that will one day search the internet
enum Xray {
    #[structopt(name = "interactive")]
    /// Starts in an interactive query mode
    Interactive,

    #[structopt(name = "search")]
    /// Performs a single search
    Search { query: Vec<String> },

    #[structopt(name = "import")]
    /// Imports raw CommonCrawl data into xray
    Import { sources: Vec<String> },

    #[structopt(name = "optimize")]
    /// Optimizes the database files
    Optimize {
        #[structopt(long = "chunk-size", default_value = "2_500_000")] chunk_size: usize,
    },

    #[structopt(name = "rebuild-index")]
    /// Only performs the final index-rebuilding step of Optimize
    RebuildIndex,

    #[structopt(name = "stats")]
    /// Prints out stats about the database
    Stats,
}

fn main() {
    let args = Xray::from_args();

    // rebuild index doesn't actually need to wait around to read the index
    let load_index = if let &Xray::RebuildIndex = &args {
        false
    } else {
        true
    };

    let storage = Storage::new("/mnt/d/tmp/", load_index);
    let mut database = Database::new(storage);
    let result = match args {
        Xray::Interactive => database.interactive(),
        Xray::Search { query } => database.search(query),
        Xray::Import { sources } => database.import(sources),
        Xray::Optimize { chunk_size } => database.optimize(chunk_size),
        Xray::RebuildIndex => database.rebuild_index(),
        Xray::Stats => database.stats(),
    };

    if let Err(error) = result {
        eprintln!("{}", error.0);
        exit(1)
    }
}
