use std::error::Error as StdErr;
use std::fmt;
use std::fmt::Debug;

type PgError = tokio_postgres::error::Error;
type PoolError<E> = bb8_postgres::bb8::RunError<E>;

#[derive(Debug)]
pub enum Error {
    Generic(Box<dyn StdErr>),
    Sql(PgError),
    InputValidation(String),
    DBConversion(String),
    Utf8(std::string::FromUtf8Error),
    Auth(String),
    Http(String),
    Pool(String),
    Pubsub(PubsubAction),
}

#[derive(Debug)]
pub enum PubsubAction {
    IgnoreAndAck,
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

impl<E: Debug> From<PoolError<E>> for Error {
    fn from(e: PoolError<E>) -> Self {
        Error::Pool(format!("{:?}", e))
    }
}
