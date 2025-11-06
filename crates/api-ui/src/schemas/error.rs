use crate::error::IntoStatusCode;
use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

// TODO: Refactor this error

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Create schema error: {source}"))]
    Create {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get schema error: {source}"))]
    Get {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Delete schema error: {source}"))]
    Delete {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Update schema error: {source}"))]
    Update {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get schemas error: {source}"))]
    List {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No id error: {source}"))]
    NoId {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    #[allow(clippy::collapsible_match)]
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. } => match &source {
                core_executor::Error::Metastore { source, .. } => match **source {
                    core_metastore::Error::SchemaAlreadyExists { .. }
                    | core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    core_metastore::Error::DatabaseNotFound { .. }
                    | core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source, .. } => match &source {
                core_metastore::Error::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source, .. } => match &source {
                core_executor::Error::Metastore { source, .. } => match **source {
                    core_metastore::Error::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source, .. } => match &source {
                core_metastore::Error::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
