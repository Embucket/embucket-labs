pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("not found")]
    ErrNotFound,

    #[error("already exists")]
    ErrAlreadyExists,

    #[error("not empty")]
    ErrNotEmpty,

    #[error("not implemented")]
    NotImplemented,

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not empty: {0}")]
    NotEmpty(String),

    #[error("failed requirement: {0}")]
    FailedRequirement(String),

    #[error("DB error: {0}")]
    DbError(String),

    #[error("Iceberg error: {0}")]
    IcebergError(#[from] iceberg::Error),
}

impl From<utils::Error> for Error {
    fn from(e: utils::Error) -> Self {
        match e {
            utils::Error::DbError(e) => Error::DbError(e.to_string()),
            utils::Error::SerializeError(e) => Error::DbError(e.to_string()),
            utils::Error::DeserializeError(e) => Error::DbError(e.to_string()),
            utils::Error::ErrNotFound => Error::ErrNotFound,
        }
    }
}
