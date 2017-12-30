#[macro_use]
extern crate nom;
extern crate rayon;
extern crate rustyline;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
extern crate whatlang;

use structopt::StructOpt;

use std::process::exit;

mod errors;
mod commoncrawl;
mod database;
mod interactive;
mod search;
mod import;
mod stats;
mod helpers;

use database::Database;

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

    let mut database = Database::new();
    let result = match args {
        Xray::Interactive => database.interactive(),
        Xray::Search { query } => database.search(query),
        Xray::Import { sources } => database.import(sources),
        Xray::Stats => database.stats(),
    };

    match result {
        Err(error) => {
            eprintln!("{}", error.0);
            exit(1)
        }
        _ => {}
    }
}
