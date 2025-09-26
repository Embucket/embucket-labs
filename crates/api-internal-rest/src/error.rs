use axum::{Json, response::IntoResponse};
use error_stack::ErrorExt;
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
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Get volume error: {error}"))]
    GetVolume {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Create volume error: {error}"))]
    CreateVolume {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Update volume error: {error}"))]
    UpdateVolume {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Delete volume error: {error}"))]
    DeleteVolume {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] List databases error: {error}"))]
    ListDatabases {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Get database error: {error}"))]
    GetDatabase {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Create database error: {error}"))]
    CreateDatabase {
        #[snafu(source)]
        error: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Get query error: {error}"))]
    GetQuery {
        #[snafu(source)]
        error: core_history::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("[InternalAPI] Get query error: {error}"))]
    GetQueryId {
        #[snafu(source)]
        error: core_history::QueryIdError,
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
    #[tracing::instrument(
        name = "api-internal-rest::Error::into_response",
        level = "info",
        fields(status_code),
        skip(self)
    )]
    fn into_response(self) -> axum::response::Response {
        tracing::error!(error_message = %self.output_msg(), "Internal API error");
        let message = self.to_string();
        let code = match self {
            Self::GetQuery { .. } => http::StatusCode::NOT_FOUND,
            Self::GetQueryId { .. } => http::StatusCode::BAD_REQUEST,
            Self::ListVolumes { error, .. }
            | Self::GetVolume { error, .. }
            | Self::CreateVolume { error, .. }
            | Self::UpdateVolume { error, .. }
            | Self::DeleteVolume { error, .. }
            | Self::ListDatabases { error, .. }
            | Self::GetDatabase { error, .. }
            | Self::CreateDatabase { error, .. } => match error {
                core_metastore::Error::TableDataExists { .. }
                | core_metastore::Error::ObjectAlreadyExists { .. }
                | core_metastore::Error::VolumeAlreadyExists { .. }
                | core_metastore::Error::DatabaseAlreadyExists { .. }
                | core_metastore::Error::SchemaAlreadyExists { .. }
                | core_metastore::Error::TableAlreadyExists { .. }
                | core_metastore::Error::VolumeInUse { .. }
                | core_metastore::Error::DatabaseInUse { .. } => http::StatusCode::CONFLICT,
                core_metastore::Error::TableRequirementFailed { .. } => {
                    http::StatusCode::UNPROCESSABLE_ENTITY
                }
                core_metastore::Error::VolumeValidationFailed { .. }
                | core_metastore::Error::VolumeMissingCredentials { .. }
                | core_metastore::Error::Validation { .. } => http::StatusCode::BAD_REQUEST,
                core_metastore::Error::CloudProviderNotImplemented { .. } => {
                    http::StatusCode::PRECONDITION_FAILED
                }
                core_metastore::Error::VolumeNotFound { .. }
                | core_metastore::Error::DatabaseNotFound { .. }
                | core_metastore::Error::SchemaNotFound { .. }
                | core_metastore::Error::TableNotFound { .. }
                | core_metastore::Error::ObjectNotFound { .. } => http::StatusCode::NOT_FOUND,
                core_metastore::Error::ObjectStore { .. }
                | core_metastore::Error::ObjectStorePath { .. }
                | core_metastore::Error::CreateDirectory { .. }
                | core_metastore::Error::SlateDB { .. }
                | core_metastore::Error::UtilSlateDB { .. }
                | core_metastore::Error::Iceberg { .. }
                | core_metastore::Error::IcebergSpec { .. }
                | core_metastore::Error::Serde { .. }
                | core_metastore::Error::TableMetadataBuilder { .. }
                | core_metastore::Error::TableObjectStoreNotFound { .. }
                | core_metastore::Error::UrlParse { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            },
        };

        // Record the result as part of the current span.
        tracing::Span::current().record("status_code", code.as_u16());

        let error = ErrorResponse {
            message,
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
