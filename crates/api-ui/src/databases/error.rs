use crate::error::IntoStatusCode;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DatabasesAPIError {
    #[snafu(display("Create database error: {source}"))]
    Create {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get database error: {source}"))]
    Get {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete database error: {source}"))]
    Delete {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update database error: {source}"))]
    Update {
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
