use http::StatusCode;
use snafu::prelude::*;

pub type HttpRequestResult<T> = std::result::Result<T, HttpRequestError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum HttpRequestError {
    #[snafu(display("HTTP request error: {message}, status code: {status}"))]
    HttpRequest { message: String, status: StatusCode },

    #[snafu(display("Invalid header value: {source}"))]
    InvalidHeaderValue {
        source: http::header::InvalidHeaderValue,
    },

    #[snafu(display("Authenticated request error: {message}"))]
    AuthenticatedRequest { message: String },

    #[snafu(display("Serialize error: {source}"))]
    Serialize { source: serde_json::Error },
}
