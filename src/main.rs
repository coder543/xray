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
use std::mem::discriminant;

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
enum XrayCmd {
    #[structopt(name = "interactive")]
    /// Starts in an interactive query mode
    Interactive,

    #[structopt(name = "search")]
    /// Performs a single search
    Search { query: Vec<String> },

    #[structopt(name = "import")]
    /// Imports raw CommonCrawl data into xray
    Import {
        #[structopt(long = "chunk-size", default_value = "36")]
        /// The number of import files to be processed in parallel
        chunk_size: usize,
        sources: Vec<String>,
    },

    #[structopt(name = "optimize")]
    /// Optimizes the database files
    Optimize {
        #[structopt(long = "chunk-size", default_value = "2500000")]
        /// The number of words per optimization chunk
        chunk_size: usize,
    },

    #[structopt(name = "rebuild-index")]
    /// Only performs the final index-rebuilding step of Optimize
    RebuildIndex,

    #[structopt(name = "stats")]
    /// Prints out stats about the database
    Stats,
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "xray")]
/// xray is a primitive search engine that will one day search the internet
struct Xray {
    #[structopt(short = "d", long = "data-dir", default_value = "/mnt/d/tmp/")]
    /// the data directory to store the indexed data in
    data_dir: String,

    #[structopt(subcommand)] command: XrayCmd,
}

fn main() {
    use XrayCmd::*;

    let args = Xray::from_args();

    // rebuild index doesn't actually need to wait around to read the index
    let load_index = discriminant(&RebuildIndex) != discriminant(&args.command);
    let storage = Storage::new(&args.data_dir, load_index);
    let mut database = Database::new(storage);

    let result = match args.command {
        Interactive => database.interactive(),
        Search { query } => database.search(query),
        Import {
            chunk_size,
            sources,
        } => database.import(sources, chunk_size),
        Optimize { chunk_size } => database.optimize(chunk_size),
        RebuildIndex => database.rebuild_index(),
        Stats => database.stats(),
    };

    if let Err(error) = result {
        eprintln!("{}", error.0);
        exit(1)
    }
}
