use crate::error::IntoStatusCode;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Create volumes error: {source}"))]
    Create {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get volume error: {source}"))]
    Get {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete volume error: {source}"))]
    Delete {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update volume error: {source}"))]
    Update {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get volumes error: {source}"))]
    List {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. } => match &source {
                core_metastore::Error::VolumeAlreadyExists { .. }
                | core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source, .. } | Self::Delete { source, .. } => match &source {
                core_metastore::Error::UtilSlateDB { .. }
                | core_metastore::Error::ObjectNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source, .. } => match &source {
                core_metastore::Error::ObjectNotFound { .. }
                | core_metastore::Error::VolumeNotFound { .. } => StatusCode::NOT_FOUND,
                core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
