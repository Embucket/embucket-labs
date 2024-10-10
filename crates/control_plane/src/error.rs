use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("not found")]
    ErrNotFound,

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not empty: {0}")]
    NotEmpty(String),
}

impl From<utils::Error> for Error {
    fn from(e: utils::Error) -> Self {
        match e {
            utils::Error::DbError(e) => Error::InvalidInput(e.to_string()),
            utils::Error::SerializeError(e) => Error::InvalidInput(e.to_string()),
            utils::Error::DeserializeError(e) => Error::InvalidInput(e.to_string()),
            utils::Error::ErrNotFound => Error::ErrNotFound,
        }
    }
}
