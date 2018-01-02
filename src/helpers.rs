#[allow(unused)]
use std::ascii::AsciiExt;
use std::time::Duration;

pub trait ReadableDuration {
    fn readable(&self) -> String;
}

impl ReadableDuration for Duration {
    fn readable(&self) -> String {
        let total = self.as_secs() as f64 + f64::from(self.subsec_nanos()) / 1_000_000_000.0;
        if total < 0.000001 {
            format!("{} ns", total * 1000.0 * 1000.0 * 1000.0)
        } else if total < 0.001 {
            format!("{} us", total * 1000.0 * 1000.0)
        } else if total < 1.0 {
            format!("{} ms", total * 1000.0)
        } else {
            format!("{} secs", total)
        }
    }
}

pub fn is_canonical(x: &char) -> bool {
    let x = *x;
    !(x == '.' || x == '\'' || x == '?' || x == '!' || x == ',' || x == '(' || x == ')' || x == '$'
        || x == '&' || x == '[' || x == ']' || x == '\'' || x == '"' || x == ':' || x == ';'
        || x == '@' || x == '|')
}

pub fn canonicalize(word: &str) -> Option<String> {
    if word.len() > 2 && word.len() < 25 {
        let mut output = String::with_capacity(word.len());
        word.chars()
            .filter(is_canonical)
            .flat_map(|c| c.to_lowercase())
            .for_each(|c| output.push(c));

        if output.len() > 2 {
            return Some(output);
        }
    }
    None
}
