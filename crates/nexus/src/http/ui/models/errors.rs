use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found")]
    NotFound,
    #[error("internal server error")]
    InternalServerError,
    #[error("already exists")]
    AlreadyExists,
    #[error("not empty")]
    NotEmpty,
    #[error("unprocessable entity")]
    UnprocessableEntity,
    #[error("not implemented")]
    Implemented,
    #[error("DB error: {0}")]
    DbError(String),
    #[error("Iceberg error: {0}")]
    IcebergError(#[from] iceberg::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            Error::DbError(ref e) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            Error::InternalServerError => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            Error::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        (status, error_message).into_response()
    }
}

impl From<utils::Error> for Error {
    fn from(e: utils::Error) -> Self {
        match e {
            utils::Error::DbError(e) => Error::DbError(e.to_string()),
            utils::Error::SerializeError(e) => Error::DbError(e.to_string()),
            utils::Error::DeserializeError(e) => Error::DbError(e.to_string()),
            utils::Error::ErrNotFound => Error::NotFound,
        }
    }
}
