use axum::response::IntoResponse;
use icebucket_metastore::error::MetastoreError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub struct MetastoreAPIError(pub MetastoreError);
pub type MetastoreAPIResult<T> = Result<T, MetastoreAPIError>;

impl IntoResponse for MetastoreAPIError {
    fn into_response(self) -> axum::response::Response {
        let message = (self.0.to_string(),);
        let code = match self.0 {
            MetastoreError::TableDataExists { .. } => http::StatusCode::CONFLICT,
            MetastoreError::TableRequirementFailed { .. } => http::StatusCode::UNPROCESSABLE_ENTITY,
            MetastoreError::VolumeValidationFailed { .. } => http::StatusCode::BAD_REQUEST,
            MetastoreError::VolumeMissingCredentials => http::StatusCode::BAD_REQUEST,
            MetastoreError::CloudProviderNotImplemented { .. } => http::StatusCode::PRECONDITION_FAILED,
            MetastoreError::ObjectStore { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::CreateDirectory { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::SlateDB { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::UtilSlateDB { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::ObjectAlreadyExists { .. } => http::StatusCode::CONFLICT,
            MetastoreError::ObjectNotFound { .. } => http::StatusCode::NOT_FOUND,
            MetastoreError::VolumeInUse { .. } => http::StatusCode::CONFLICT,
            MetastoreError::Iceberg { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::Serde { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::Validation { .. } => http::StatusCode::BAD_REQUEST,
        };
        (code, message).into_response()
    }
}