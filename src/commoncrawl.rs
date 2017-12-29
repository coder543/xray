use std::str::{self, FromStr};
use nom::{IResult, line_ending};

/// A struct representing a single WET blob
#[derive(Clone, Debug)]
pub struct WetRef<'a> {
    pub ref_type: &'a str,
    pub url: &'a str,
    pub date: &'a str,
    pub filename: &'a str,
    pub record_id: &'a str,
    pub refers_to: &'a str,
    pub block_digest: &'a str,
    pub content_type: &'a str,
    pub content: &'a [u8],
}

pub trait GetWetRef<'a> {
    fn next_wet_ref(&'a self) -> (WetRef<'a>, &'a Self);
}

impl<'a> GetWetRef<'a> for [u8] {
    fn next_wet_ref(&'a self) -> (WetRef<'a>, &'a [u8]) {
        match parse_wet_ref(self) {
            IResult::Done(rem, val) => (val, rem),
            IResult::Error(err) => panic!(format!("{:?}", err)),
            IResult::Incomplete(_) => panic!(),
        }
    }
}

fn is_whitespace(chr: u8) -> bool {
    match char::from(chr) {
        ' ' | '\n' | '\r' | '\t' => true,
        _ => false
    }
}

named!(take_line<&str>, map_res!(
    alt!(
          terminated!(take_until!("\r"), tag!("\r\n"))
        | terminated!(take_until!("\n"), tag!("\n"))
    ),
    str::from_utf8
));

named!(parse_type<&str>, preceded!(tag!("WARC-Type: "), take_line));
named!(parse_url<&str>, alt!(preceded!(tag!("WARC-Target-URI: "), take_line) | value!("")));
named!(parse_date<&str>, preceded!(tag!("WARC-Date: "), take_line));
named!(parse_filename<&str>, alt!(preceded!(tag!("WARC-Filename: "), take_line) | value!("")));
named!(parse_record_id<&str>, preceded!(tag!("WARC-Record-ID: "), take_line));
named!(parse_refers_to<&str>, alt!(preceded!(tag!("WARC-Refers-To: "), take_line) | value!("")));
named!(parse_digest<&str>, alt!(preceded!(tag!("WARC-Block-Digest: "), take_line) | value!("")));

named!(parse_content_type<&str>, preceded!(tag!("Content-Type: "), take_line));
named!(parse_content_length<u64>, map_res!(
    preceded!(tag!("Content-Length: "), take_line),
    FromStr::from_str
));

named!(parse_wet_ref<WetRef>, do_parse!(
    pair!(tag!("WARC/1.0"), line_ending) >>
    ref_type: parse_type >>
    url: parse_url >>
    date: parse_date >>
    filename: parse_filename >>
    record_id: parse_record_id >>
    refers_to: parse_refers_to >>
    block_digest: parse_digest >>
    content_type: parse_content_type >>
    content_length: parse_content_length >>
    tag!("\r\n") >>
    content: take!(content_length) >>
    take_while!(is_whitespace) >>
    (
        WetRef {
            ref_type,
            url,
            date,
            filename,
            record_id,
            refers_to,
            block_digest,
            content_type,
            content,
        }
    )
));