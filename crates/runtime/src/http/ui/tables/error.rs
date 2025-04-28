use crate::execution::error::ExecutionError;
use crate::http::error::ErrorResponse;
use crate::http::ui::error::IntoStatusCode;
use axum::extract::multipart;
use axum::response::IntoResponse;
use axum::Json;
use embucket_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

pub type TablesResult<T> = Result<T, TablesAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum TableError {
    #[snafu(display("Malformed multipart form data: {source}"))]
    MalformedMultipart { source: multipart::MultipartError },
    #[snafu(display("Malformed multipart file data: {source}"))]
    MalformedMultipartFileData { source: multipart::MultipartError },
    #[snafu(display("Malformed file upload request"))]
    MalformedFileUploadRequest,
    #[snafu(display("File field missing in form data"))]
    FileField,
    #[snafu(transparent)]
    Execution {
        source: crate::execution::error::ExecutionError,
    },
    #[snafu(transparent)]
    Metastore { source: MetastoreError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum TablesAPIError {
    #[snafu(display("Create table error: {source}"))]
    CreateUpload { source: TableError },
    #[snafu(display("Get table error: {source}"))]
    GetExecution { source: ExecutionError },
    #[snafu(display("Get table error: {source}"))]
    GetMetastore { source: MetastoreError },
}

// Select which status code to return.
impl IntoStatusCode for TablesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::CreateUpload { source } => match &source {
                TableError::Metastore { source } => match &source {
                    MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    MetastoreError::DatabaseNotFound { .. }
                    | MetastoreError::SchemaNotFound { .. }
                    | MetastoreError::TableNotFound { .. }
                    | MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                TableError::MalformedMultipart { .. }
                | TableError::MalformedMultipartFileData { .. } => StatusCode::BAD_REQUEST,
                TableError::Execution {
                    source: ExecutionError::DataFusion { .. },
                } => StatusCode::UNPROCESSABLE_ENTITY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::GetExecution { source } => match &source {
                ExecutionError::TableNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::GetMetastore { source } => match &source {
                MetastoreError::TableNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

// generic
impl IntoResponse for TablesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
