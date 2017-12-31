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

    #[structopt(name = "stats")]
    /// Prints out stats about the database
    Stats,
}

fn main() {
    let args = Xray::from_args();

    let storage = Storage::new("/mnt/d/tmp/");
    let mut database = Database::new(storage);
    let result = match args {
        Xray::Interactive => database.interactive(),
        Xray::Search { query } => database.search(query),
        Xray::Import { sources } => database.import(sources),
        Xray::Stats => database.stats(),
    };

    if let Err(error) = result {
        eprintln!("{}", error.0);
        exit(1)
    }

    // database.interactive().unwrap();
}
