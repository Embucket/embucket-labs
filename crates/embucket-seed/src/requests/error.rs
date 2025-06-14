use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

pub type HttpRequestResult<T> = std::result::Result<T, HttpRequestError>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum HttpRequestError {
    #[snafu(display("HTTP request error: {message}, status code: {status}"))]
    HttpRequest {
        message: String,
        status: StatusCode,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid header value: {error}"))]
    InvalidHeaderValue {
        #[snafu(source)]
        error: http::header::InvalidHeaderValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Authenticated request error: {message}"))]
    AuthenticatedRequest {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Serialize error: {error}"))]
    Serialize {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
}
