use crate::error::IntoStatusCode;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum VolumesAPIError {
    #[snafu(display("Create volumes error: {source}"))]
    Create {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get volume error: {source}"))]
    Get {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete volume error: {source}"))]
    Delete {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update volume error: {source}"))]
    Update {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get volumes error: {source}"))]
    List {
        source: ExecutionError,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for VolumesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. } => match &source {
                MetastoreError::VolumeAlreadyExists { .. }
                | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source, .. } | Self::Delete { source, .. } => match &source {
                MetastoreError::UtilSlateDB { .. } | MetastoreError::ObjectNotFound { .. } => {
                    StatusCode::NOT_FOUND
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source, .. } => match &source {
                MetastoreError::ObjectNotFound { .. } | MetastoreError::VolumeNotFound { .. } => {
                    StatusCode::NOT_FOUND
                }
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
