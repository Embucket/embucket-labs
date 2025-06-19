use axum::{Json, response::IntoResponse};
use core_metastore::error::MetastoreError;
use error_stack_trace;
use http;
use serde::{Deserialize, Serialize};
use snafu::Location;
use snafu::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Operation {
    CreateNamespace,
    GetNamespace,
    DeleteNamespace,
    ListNamespaces,
    CreateTable,
    RegisterTable,
    CommitTable,
    GetTable,
    DeleteTable,
    ListTables,
}

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("[IcebergAPI] Operation '{operation:?}' failed. Metastore error: {source}"))]
    Metastore {
        operation: Operation,
        source: MetastoreError,
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
            Self::Metastore { source, .. } => source,
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
