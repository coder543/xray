use std::str::{self, FromStr};

use nom::{line_ending, IResult};
use whatlang::*;

/// A struct representing a single WET blob
#[derive(Clone, Debug)]
pub enum WetRef<'a> {
    WarcInfo {
        date: &'a str,
        filename: &'a str,
        record_id: &'a str,
        content_type: &'a str,
        content: &'a str,
    },
    Conversion {
        url: &'a str,
        date: &'a str,
        record_id: &'a str,
        refers_to: &'a str,
        block_digest: &'a str,
        content_type: &'a str,
        content_lang: Option<Info>,
        content: &'a str,
    },
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
        _ => false,
    }
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(take_line<&str>, map_res!(
    alt!(
          terminated!(take_until!("\r"), tag!("\r\n"))
        | terminated!(take_until!("\n"), tag!("\n"))
    ),
    str::from_utf8
));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_url<&str>, preceded!(tag!("WARC-Target-URI: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_date<&str>, preceded!(tag!("WARC-Date: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_filename<&str>, preceded!(tag!("WARC-Filename: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_record_id<&str>, preceded!(tag!("WARC-Record-ID: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_refers_to<&str>, preceded!(tag!("WARC-Refers-To: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_digest<&str>, preceded!(tag!("WARC-Block-Digest: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_content_type<&str>, preceded!(tag!("Content-Type: "), take_line));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_content_length<u64>, map_res!(
    preceded!(tag!("Content-Length: "), take_line),
    FromStr::from_str
));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_warcinfo<WetRef>, do_parse!(
    pair!(tag!("WARC-Type: warcinfo"), line_ending) >>
    date: parse_date >>
    filename: parse_filename >>
    record_id: parse_record_id >>
    content_type: parse_content_type >>
    content_length: parse_content_length >>
    tag!("\r\n") >>
    content: take_str!(content_length) >>
    take_while!(is_whitespace) >>
    (
        WetRef::WarcInfo {
            date,
            filename,
            record_id,
            content_type,
            content,
        }
    )
));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_conversion<WetRef>, do_parse!(
    pair!(tag!("WARC-Type: conversion"), line_ending) >>
    url: parse_url >>
    date: parse_date >>
    record_id: parse_record_id >>
    refers_to: parse_refers_to >>
    block_digest: parse_digest >>
    content_type: parse_content_type >>
    content_length: parse_content_length >>
    tag!("\r\n") >>
    content: take_str!(content_length) >>
    take_while!(is_whitespace) >>
    (
        WetRef::Conversion {
            url,
            date,
            record_id,
            refers_to,
            block_digest,
            content_type,
            content_lang: detect(content),
            content,
        }
    )
));

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(parse_wet_ref<WetRef>, do_parse!(
    pair!(tag!("WARC/1.0"), line_ending) >>
    result: alt!(parse_conversion | parse_warcinfo) >>
    (result)
));
