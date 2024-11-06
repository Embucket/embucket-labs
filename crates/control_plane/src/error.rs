#[warn(dead_code)]
use quick_xml::de::from_str;
use rusoto_core::RusotoError;
use serde::Deserialize;
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

    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),

    #[error("datafusion error: {0}")]
    DataFusionError(String),
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

#[derive(Debug, Deserialize)]
struct S3Error {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
}

pub fn extract_error_message<T>(e: &RusotoError<T>) -> Option<String> {
    if let RusotoError::Unknown(response) = e {
        if let Ok(s3_error) = from_str::<S3Error>(&String::from_utf8_lossy(&response.body)) {
            return Some(s3_error.code + s3_error.message.as_str());
        }
    }
    None
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Error::DataFusionError(err.to_string())
    }
}
