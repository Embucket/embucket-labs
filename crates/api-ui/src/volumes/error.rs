use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

pub type VolumesResult<T> = Result<T, VolumesAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum VolumesAPIError {
    #[snafu(display("Create volumes error: {source}"))]
    Create { source: MetastoreError },
    #[snafu(display("Get volume error: {source}"))]
    Get { source: MetastoreError },
    #[snafu(display("Delete volume error: {source}"))]
    Delete { source: MetastoreError },
    #[snafu(display("Update volume error: {source}"))]
    Update { source: MetastoreError },
    #[snafu(display("Get volume error: {source}"))]
    List { source: ExecutionError },
}

// Add conversion from Box<MetastoreError> to VolumesAPIError
impl From<Box<MetastoreError>> for VolumesAPIError {
    fn from(boxed_error: Box<MetastoreError>) -> Self {
        let error = *boxed_error;
        match error {
            err @ MetastoreError::VolumeNotFound { .. } => Self::Get { source: err },
            err @ MetastoreError::VolumeAlreadyExists { .. } | err => Self::Create { source: err },
        }
    }
}

// Select which status code to return.
impl IntoStatusCode for VolumesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                MetastoreError::VolumeAlreadyExists { .. }
                | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } | Self::Delete { source } => match &source {
                MetastoreError::UtilSlateDB { .. } | MetastoreError::ObjectNotFound => {
                    StatusCode::NOT_FOUND
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &source {
                MetastoreError::ObjectNotFound | MetastoreError::VolumeNotFound { .. } => {
                    StatusCode::NOT_FOUND
                }
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// TODO: make it reusable by other *APIError
impl IntoResponse for VolumesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
