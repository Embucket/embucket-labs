use crate::http::error::ErrorResponse;
use axum::{response::IntoResponse, Json};
use embucket_metastore::error::MetastoreError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub struct MetastoreAPIError(pub MetastoreError);
pub type MetastoreAPIResult<T> = Result<T, MetastoreAPIError>;

impl IntoResponse for MetastoreAPIError {
    fn into_response(self) -> axum::response::Response {
        let message = (self.0.to_string(),);
        let code = match self.0 {
            MetastoreError::TableDataExists { .. }
            | MetastoreError::ObjectAlreadyExists { .. }
            | MetastoreError::VolumeAlreadyExists { .. }
            | MetastoreError::DatabaseAlreadyExists { .. }
            | MetastoreError::SchemaAlreadyExists { .. }
            | MetastoreError::TableAlreadyExists { .. }
            | MetastoreError::VolumeInUse { .. } => http::StatusCode::CONFLICT,
            MetastoreError::TableRequirementFailed { .. } => http::StatusCode::UNPROCESSABLE_ENTITY,
            MetastoreError::VolumeValidationFailed { .. }
            | MetastoreError::VolumeMissingCredentials
            | MetastoreError::Validation { .. } => http::StatusCode::BAD_REQUEST,
            MetastoreError::CloudProviderNotImplemented { .. } => {
                http::StatusCode::PRECONDITION_FAILED
            }
            MetastoreError::VolumeNotFound { .. }
            | MetastoreError::DatabaseNotFound { .. }
            | MetastoreError::SchemaNotFound { .. }
            | MetastoreError::TableNotFound { .. }
            | MetastoreError::ObjectNotFound => http::StatusCode::NOT_FOUND,
            MetastoreError::ObjectStore { .. }
            | MetastoreError::ObjectStorePath { .. }
            | MetastoreError::CreateDirectory { .. }
            | MetastoreError::SlateDB { .. }
            | MetastoreError::UtilSlateDB { .. }
            | MetastoreError::Iceberg { .. }
            | MetastoreError::Serde { .. }
            | MetastoreError::TableMetadataBuilder { .. }
            | MetastoreError::TableObjectStoreNotFound { .. }
            | MetastoreError::UrlParse { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
        };

        let error = ErrorResponse {
            message: message.0,
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
