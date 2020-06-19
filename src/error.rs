use std::error::Error as StdErr;
use std::fmt;

type PgError = postgres::error::Error;

#[derive(Debug)]
pub enum Error {
    Generic(Box<dyn StdErr>),
    Sql(PgError),
    InputValidation(String),
    DBConversion(String),
    Utf8(std::string::FromUtf8Error),
    Auth(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<std::env::VarError> for Error {
    fn from(e: std::env::VarError) -> Self {
        Error::Generic(Box::new(e))
    }
}

impl From<PgError> for Error {
    fn from(e: PgError) -> Self {
        Error::Sql(e)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::Utf8(e)
    }
}
