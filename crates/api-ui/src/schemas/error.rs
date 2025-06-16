use crate::error::IntoStatusCode;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

// TODO: Refactor this error

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum SchemasAPIError {
    #[snafu(display("Create schema error: {source}"))]
    Create {
        source: ExecutionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get schema error: {source}"))]
    Get {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Delete schema error: {source}"))]
    Delete {
        source: ExecutionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Update schema error: {source}"))]
    Update {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get schemas error: {source}"))]
    List {
        source: ExecutionError,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for SchemasAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. } => match &source {
                ExecutionError::Metastore { source, .. } => match **source {
                    MetastoreError::SchemaAlreadyExists { .. }
                    | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    MetastoreError::DatabaseNotFound { .. } | MetastoreError::Validation { .. } => {
                        StatusCode::BAD_REQUEST
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source, .. } => match &source {
                MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source, .. } => match &source {
                ExecutionError::Metastore { source, .. } => match **source {
                    MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source, .. } => match &source {
                MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
