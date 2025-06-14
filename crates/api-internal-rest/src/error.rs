use axum::{Json, response::IntoResponse};
use core_metastore::error::MetastoreError;
use error_stack_trace;
use http;
use serde::{Deserialize, Serialize};
use snafu::Location;
use snafu::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("[InternalAPI] List volumes error: {error}"))]
    ListVolumes {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Get volume error: {error}"))]
    GetVolume {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Create volume error: {error}"))]
    CreateVolume {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Update volume error: {error}"))]
    UpdateVolume {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Delete volume error: {error}"))]
    DeleteVolume {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] List databases error: {error}"))]
    ListDatabases {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Get database error: {error}"))]
    GetDatabase {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Create database error: {error}"))]
    CreateDatabase {
        #[snafu(source)]
        error: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let metastore_error = match self {
            Self::ListVolumes { error, .. }
            | Self::GetVolume { error, .. }
            | Self::CreateVolume { error, .. }
            | Self::UpdateVolume { error, .. }
            | Self::DeleteVolume { error, .. }
            | Self::ListDatabases { error, .. }
            | Self::GetDatabase { error, .. }
            | Self::CreateDatabase { error, .. } => error,
        };

        let message = metastore_error.to_string();
        let code = match metastore_error {
            MetastoreError::TableDataExists { .. }
            | MetastoreError::ObjectAlreadyExists { .. }
            | MetastoreError::VolumeAlreadyExists { .. }
            | MetastoreError::DatabaseAlreadyExists { .. }
            | MetastoreError::SchemaAlreadyExists { .. }
            | MetastoreError::TableAlreadyExists { .. }
            | MetastoreError::VolumeInUse { .. } => http::StatusCode::CONFLICT,
            MetastoreError::TableRequirementFailed { .. } => http::StatusCode::UNPROCESSABLE_ENTITY,
            MetastoreError::VolumeValidationFailed { .. }
            | MetastoreError::VolumeMissingCredentials { .. }
            | MetastoreError::Validation { .. } => http::StatusCode::BAD_REQUEST,
            MetastoreError::CloudProviderNotImplemented { .. } => {
                http::StatusCode::PRECONDITION_FAILED
            }
            MetastoreError::VolumeNotFound { .. }
            | MetastoreError::DatabaseNotFound { .. }
            | MetastoreError::SchemaNotFound { .. }
            | MetastoreError::TableNotFound { .. }
            | MetastoreError::ObjectNotFound { .. } => http::StatusCode::NOT_FOUND,
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
            message,
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
