use crate::error::ErrorResponse;
use axum::Json;
use axum::response::IntoResponse;
use http::Error;
use http::StatusCode;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ArchiveError {
    #[snafu(display("File not found: {path}"))]
    NotFound { path: String },

    #[snafu(display("Response body error: {source}"))]
    ResponseBody { source: Error },

    #[snafu(display("Bad archive: {source}"))]
    BadArchive { source: std::io::Error },

    #[snafu(display("Entry path is not a valid Unicode: {source}"))]
    NonUnicodeEntryPathInArchive { source: std::io::Error },

    #[snafu(display("Entry data read error: {source}"))]
    ReadEntryData { source: std::io::Error },
}

pub struct HandlerError(pub StatusCode, pub ArchiveError);

pub type Result<T> = std::result::Result<T, HandlerError>;

impl IntoResponse for HandlerError {
    fn into_response(self) -> axum::response::Response {
        let code = self.0;
        let error = self.1;
        let error = ErrorResponse {
            message: error.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
