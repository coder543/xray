use database::Database;
use whatlang::*;

impl Database {
    pub fn search(&self, query: Vec<String>) -> Result<(), String> {
        // for detecting query language, only a limited set are supported to reduce false positives on short strings
        let detector = Detector::with_whitelist(vec![Lang::Eng, Lang::Spa, Lang::Fra, Lang::Cmn, Lang::Jpn, Lang::Kor, Lang::Rus]);
        println!("{:?}", detector.detect(&query[0]));
        Ok(())
        // Err("Search has not been implemented yet!")?
    }
}