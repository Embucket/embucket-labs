use crate::error::IntoStatusCode;
use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Create database query error: {source}"))]
    CreateQuery {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Create database error: {source}"))]
    Create {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get database error: {source}"))]
    Get {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete database error: {source}"))]
    Delete {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update database error: {source}"))]
    Update {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get databases error: {source}"))]
    List {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Database {database} not found"))]
    DatabaseNotFound {
        database: String,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::CreateQuery { source, .. } => match &source {
                core_executor::Error::Metastore { source, .. } => match **source {
                    core_metastore::Error::DatabaseAlreadyExists { .. }
                    | core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    core_metastore::Error::VolumeNotFound { .. }
                    | core_metastore::Error::DatabaseNotFound { .. }
                    | core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Create { source, .. } => match &source {
                core_metastore::Error::DatabaseAlreadyExists { .. }
                | core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                core_metastore::Error::VolumeNotFound { .. }
                | core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source, .. } | Self::Delete { source, .. } => match &source {
                core_metastore::Error::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source, .. } => match &source {
                core_metastore::Error::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
                core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DatabaseNotFound { .. } => StatusCode::NOT_FOUND,
        }
    }
}
