use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;
use stack_error;
use stack_error_proc::stack_trace_debug;

pub type DatabasesResult<T> = Result<T, DatabasesAPIError>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum DatabasesAPIError {
    #[snafu(display("Create database error: {source}"))]
    Create {
        #[snafu(source(from(Box<MetastoreError>, |e: Box<MetastoreError>| *e)))]
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get database error: {source}"))]
    Get {
        #[snafu(source(from(Box<MetastoreError>, |e: Box<MetastoreError>| *e)))]
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete database error: {source}"))]
    Delete {
        #[snafu(source(from(Box<MetastoreError>, |e: Box<MetastoreError>| *e)))]
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update database error: {source}"))]
    Update {
        #[snafu(source(from(Box<MetastoreError>, |e: Box<MetastoreError>| *e)))]
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get databases error: {source}"))]
    List {
        source: ExecutionError,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for DatabasesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. } => match &source {
                MetastoreError::DatabaseAlreadyExists { .. }
                | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                MetastoreError::VolumeNotFound { .. } | MetastoreError::Validation { .. } => {
                    StatusCode::BAD_REQUEST
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source, .. } | Self::Delete { source, .. } => match &source {
                MetastoreError::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source, .. } => match &source {
                MetastoreError::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// TODO: make it reusable by other *APIError
impl IntoResponse for DatabasesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
