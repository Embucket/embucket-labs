use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

pub type SchemasResult<T> = Result<T, SchemasAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SchemasAPIError {
    #[snafu(display("Create schema error: {source}"))]
    Create { source: ExecutionError },
    #[snafu(display("Get schema error: {source}"))]
    Get { source: Box<MetastoreError> },
    #[snafu(display("Delete schema error: {source}"))]
    Delete { source: ExecutionError },
    #[snafu(display("Update schema error: {source}"))]
    Update { source: Box<MetastoreError> },
    #[snafu(display("Get schemas error: {source}"))]
    List { source: ExecutionError },
}

// Select which status code to return.
impl IntoStatusCode for SchemasAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                ExecutionError::Metastore { source } => match &**source {
                    MetastoreError::SchemaAlreadyExists { .. }
                    | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    MetastoreError::DatabaseNotFound { .. } | MetastoreError::Validation { .. } => {
                        StatusCode::BAD_REQUEST
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } => match &**source {
                MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source } => match &source {
                ExecutionError::Metastore { source } => match &**source {
                    MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &**source {
                MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// TODO: make it reusable by other *APIError
impl IntoResponse for SchemasAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
