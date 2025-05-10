use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

pub type NavigationTreesResult<T> = Result<T, NavigationTreesAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum NavigationTreesAPIError {
    #[snafu(display("Get navigation trees error: {source}"))]
    Get { source: MetastoreError },

    #[snafu(display("Execution error: {source}"))]
    Execution { source: ExecutionError },
}

// Select which status code to return.
impl IntoStatusCode for NavigationTreesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Get { .. } | Self::Execution { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// generic
impl IntoResponse for NavigationTreesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
