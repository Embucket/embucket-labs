use axum::{Json, http, response::IntoResponse};
use error_stack_trace;
use http::header::InvalidHeaderValue;
use http::{StatusCode, header::MaxSizeReached};
use serde::{Deserialize, Serialize};
use snafu::Location;
use snafu::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Can't add header to response: {error}"))]
    ResponseHeader {
        #[snafu(source)]
        error: InvalidHeaderValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Set-Cookie error: {error}"))]
    SetCookie {
        #[snafu(source)]
        error: MaxSizeReached,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Execution error: {error}"))]
    Execution {
        #[snafu(source)]
        error: core_executor::Error,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        let message = self.to_string();
        let code = StatusCode::INTERNAL_SERVER_ERROR;

        let error = ErrorResponse {
            message,
            status_code: code.as_u16(),
        };

        (code, Json(error)).into_response()
    }
}
