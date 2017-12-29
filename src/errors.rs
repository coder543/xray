use std;

#[derive(Debug)]
pub struct StrError(pub String);

impl std::convert::From<std::io::Error> for StrError {
    fn from(error: std::io::Error) -> Self {
        StrError(format!("{:#?}", error))
    }
}

impl std::convert::From<String> for StrError {
    fn from(error: String) -> Self {
        StrError(error)
    }
}

impl<'a> std::convert::From<&'a str> for StrError {
    fn from(error: &'a str) -> Self {
        StrError(error.to_string())
    }
}
