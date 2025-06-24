use crate::error::IntoStatusCode;
use axum::extract::multipart;
use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Upload file error: {source}"))]
    UploadFile {
        source: TableError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get table statistics error: {source}"))]
    GetTableStatistics {
        source: TableError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get table columns error: {source}"))]
    GetTableColumns {
        source: TableError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get table rows error: {source}"))]
    GetTablePreviewData {
        source: TableError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get tables error: {source}"))]
    GetTables {
        source: TableError,
        #[snafu(implicit)]
        location: Location,
    },
}

// kind of reusable table error
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum TableError {
    #[snafu(display("Malformed multipart form data: {error}"))]
    MalformedMultipart {
        #[snafu(source)]
        error: multipart::MultipartError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Malformed multipart file data: {error}"))]
    MalformedMultipartFileData {
        #[snafu(source)]
        error: multipart::MultipartError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Malformed file upload request"))]
    MalformedFileUploadRequest {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("File field missing in form data"))]
    FileField {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Execution error: {source}"))]
    Execution {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::UploadFile { source, .. }
            | Self::GetTableStatistics { source, .. }
            | Self::GetTableColumns { source, .. }
            | Self::GetTablePreviewData { source, .. }
            | Self::GetTables { source, .. } => match &source {
                TableError::Metastore { source, .. } => match &source {
                    core_metastore::Error::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    core_metastore::Error::DatabaseNotFound { .. }
                    | core_metastore::Error::SchemaNotFound { .. }
                    | core_metastore::Error::TableNotFound { .. }
                    | core_metastore::Error::Validation { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                TableError::Execution { source, .. } => match &source {
                    core_executor::Error::TableNotFound { .. } => StatusCode::NOT_FOUND,
                    core_executor::Error::DataFusion { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                    core_executor::Error::Arrow { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                TableError::FileField { .. }
                | TableError::MalformedFileUploadRequest { .. }
                | TableError::MalformedMultipart { .. }
                | TableError::MalformedMultipartFileData { .. } => StatusCode::BAD_REQUEST,
            },
        }
    }
}
