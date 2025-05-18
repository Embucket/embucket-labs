use snafu::prelude::*;
use http::StatusCode;

pub type HttpRequestResult<T> = std::result::Result<T, HttpRequestError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum HttpRequestError {
    #[snafu(display("HTTP request error: {message}, status code: {status}"))]
    HttpRequest{message: String, status: StatusCode},

    #[snafu(display("Authenticated request error: {message}"))]
    AuthenticatedRequest { message: String },

    #[snafu(display("Serialize error: {source}"))]
    Serialize { source: serde_json::Error },
}
