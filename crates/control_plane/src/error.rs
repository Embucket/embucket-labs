use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("not found")]
    ErrNotFound,

    #[error("invalid input: {0}")]
    InvalidInput(String),
}