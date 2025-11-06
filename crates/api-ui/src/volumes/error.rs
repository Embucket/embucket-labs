use crate::error::IntoStatusCode;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Create volume query error: {source}"))]
    CreateQuery {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Create volume error: {source}"))]
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
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound {
        volume: String,
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

fn core_executor_error(source: &core_executor::Error) -> StatusCode {
    match source {
        core_executor::Error::QueryExecution { source, .. } => core_executor_error(source),
        core_executor::Error::Metastore { source, .. } => match **source {
            core_metastore::Error::VolumeAlreadyExists { .. }
            | core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
            core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        core_executor::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
        core_executor::Error::VolumeFieldRequired { .. } => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Select which status code to return.
impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::CreateQuery { source, .. } => core_executor_error(source),
            Self::Create { source, .. } => match &source {
                core_metastore::Error::VolumeAlreadyExists { .. }
                | core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                core_metastore::Error::Validation { .. } => StatusCode::UNPROCESSABLE_ENTITY,
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
                core_metastore::Error::Validation { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::VolumeNotFound { .. } => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
