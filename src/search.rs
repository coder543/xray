use std::time::Instant;

use whatlang::Lang;

use database::Database;
use errors::StrError;
use helpers::ReadableDuration;

impl Database {
    pub fn search(&mut self, words: Vec<String>) -> Result<(), StrError> {
        // for detecting query language, only a limited set are supported to reduce
        // false positives on short strings

        // let detector = Detector::with_whitelist(vec![
        //     Lang::Eng,
        //     Lang::Spa,
        //     Lang::Fra,
        //     Lang::Cmn,
        //     Lang::Jpn,
        //     Lang::Kor,
        //     Lang::Rus,
        // ]);
        // let lang = detector.detect(&words.join(" "));
        // println!("{:?}", lang);

        let now = Instant::now();

        self.query(words, Some(Lang::Eng));

        let elapsed = now.elapsed().readable();
        println!("performed query in {}", elapsed);

        Ok(())
        // Err("Search has not been implemented yet!")?
    }
}
