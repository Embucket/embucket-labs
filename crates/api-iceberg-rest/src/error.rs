use axum::{Json, response::IntoResponse};
use common_proc::stack_trace_debug;
use core_metastore::error::MetastoreError;
use core_utils::Error as CoreError;
use http;
use object_store::Error as ObjectStoreError;
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use snafu::Location;
use snafu::prelude::*;
use validator::ValidationErrors;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum IcebergAPIError {
    #[snafu(display("Create namespace error: {source}"))]
    CreateNamespace {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get namespace error: {source}"))]
    GetNamespace {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Delete namespace error: {source}"))]
    DeleteNamespace {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("List namespaces error: {source}"))]
    ListNamespaces {
        source: CoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get table error: {source}"))]
    GetTable {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Delete table error: {source}"))]
    DeleteTable {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("List tables error: {source}"))]
    ListTables {
        source: CoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Create table error: {source}"))]
    CreateTable {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Commit table error: {source}"))]
    CommitTable {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    // TODO: Make following errors more specific and contextually appropriate
    #[snafu(display("Object store error: {error}"))]
    ObjectStore {
        #[snafu(source)]
        error: ObjectStoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Serde error: {error}"))]
    Serde {
        #[snafu(source)]
        error: SerdeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Validation error: {error}"))]
    Validation {
        #[snafu(source)]
        error: ValidationErrors,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type IcebergAPIResult<T> = Result<T, IcebergAPIError>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoResponse for IcebergAPIError {
    fn into_response(self) -> axum::response::Response {
        let metastore_error = match self {
            Self::Metastore { source } => source,
        };

        let message = metastore_error.to_string();
        let code = match *metastore_error {
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
