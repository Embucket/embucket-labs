use axum::Json;
use axum::{response::IntoResponse, response::Response};
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum UIError {
    #[snafu(transparent)]
    Execution { source: ExecutionError },
    #[snafu(transparent)]
    Metastore { source: MetastoreError },
}
pub type UIResult<T> = Result<T, UIError>;

pub(crate) trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoResponse for UIError {
    fn into_response(self) -> Response<axum::body::Body> {
        match self {
            Self::Execution { source } => exec_error_into_response(&source),
            Self::Metastore { source } => metastore_error_into_response(&source),
        }
    }
}

fn metastore_error_into_response(error: &MetastoreError) -> axum::response::Response {
    let message = error.to_string();
    let code = match error {
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
        MetastoreError::CloudProviderNotImplemented { .. } => http::StatusCode::PRECONDITION_FAILED,
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

fn exec_error_into_response(error: &ExecutionError) -> axum::response::Response {
    let status_code = match error {
        ExecutionError::RegisterUDF { .. }
        | ExecutionError::RegisterUDAF { .. }
        | ExecutionError::InvalidTableIdentifier { .. }
        | ExecutionError::InvalidSchemaIdentifier { .. }
        | ExecutionError::InvalidFilePath { .. }
        | ExecutionError::InvalidBucketIdentifier { .. }
        | ExecutionError::TableProviderNotFound { .. }
        | ExecutionError::MissingDataFusionSession { .. }
        | ExecutionError::Utf8 { .. }
        | ExecutionError::VolumeNotFound { .. }
        | ExecutionError::ObjectStore { .. }
        | ExecutionError::ObjectAlreadyExists { .. }
        | ExecutionError::UnsupportedFileFormat { .. }
        | ExecutionError::RefreshCatalogList { .. }
        | ExecutionError::UrlParse { .. }
        | ExecutionError::JobError { .. }
        | ExecutionError::UploadFailed { .. } => http::StatusCode::BAD_REQUEST,
        ExecutionError::Arrow { .. }
        | ExecutionError::SerdeParse { .. }
        | ExecutionError::S3Tables { .. }
        | ExecutionError::Iceberg { .. }
        | ExecutionError::CatalogListDowncast { .. }
        | ExecutionError::CatalogDownCast { .. }
        | ExecutionError::RegisterCatalog { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
        ExecutionError::DatabaseNotFound { .. }
        | ExecutionError::TableNotFound { .. }
        | ExecutionError::SchemaNotFound { .. }
        | ExecutionError::CatalogNotFound { .. }
        | ExecutionError::Metastore { .. }
        | ExecutionError::DataFusion { .. }
        | ExecutionError::DataFusionQuery { .. } => http::StatusCode::OK,
    };

    let message = match &error {
        ExecutionError::DataFusion { error, location } => {
            format!("DataFusion error: {error}, location: {location}")
        }
        ExecutionError::DataFusionQuery {
            error,
            query,
            location,
        } => {
            format!("DataFusion error: {error}, query: {query}, location: {location}")
        }
        ExecutionError::InvalidTableIdentifier { ident, location } => {
            format!("Invalid table identifier: {ident}, location: {location}")
        }
        ExecutionError::InvalidSchemaIdentifier { ident, location } => {
            format!("Invalid schema identifier: {ident}, location: {location}")
        }
        ExecutionError::InvalidFilePath { path, location } => {
            format!("Invalid file path: {path}, location: {location}")
        }
        ExecutionError::InvalidBucketIdentifier { ident, location } => {
            format!("Invalid bucket identifier: {ident}, location: {location}")
        }
        ExecutionError::Arrow { error, location } => {
            format!("Arrow error: {error}, location: {location}")
        }
        ExecutionError::TableProviderNotFound {
            table_name,
            location,
        } => {
            format!("No Table Provider found for table: {table_name}, location: {location}")
        }
        ExecutionError::MissingDataFusionSession { id, location } => {
            format!("Missing DataFusion session for id: {id}, location: {location}")
        }
        ExecutionError::Utf8 { error, location } => {
            format!("Error encoding UTF8 string: {error}, location: {location}")
        }
        ExecutionError::Metastore { source, location } => {
            format!("Metastore error: {source}, location: {location}")
        }
        ExecutionError::DatabaseNotFound { db, location } => {
            format!("Database not found: {db}, location: {location}")
        }
        ExecutionError::TableNotFound { table, location } => {
            format!("Table not found: {table}, location: {location}")
        }
        ExecutionError::SchemaNotFound { schema, location } => {
            format!("Schema not found: {schema}, location: {location}")
        }
        ExecutionError::VolumeNotFound { volume, location } => {
            format!("Volume not found: {volume}, location: {location}")
        }
        ExecutionError::ObjectStore { error, location } => {
            format!("Object store error: {error}, location: {location}")
        }
        ExecutionError::ObjectAlreadyExists {
            type_name,
            name,
            location,
        } => format!(
            "Object of type {type_name} with name {name} already exists, location: {location}"
        ),
        ExecutionError::UnsupportedFileFormat { format, location } => {
            format!("Unsupported file format {format}, location: {location}")
        }
        ExecutionError::RefreshCatalogList { source, location } => {
            format!("Refresh Catalog List error: {source}, location: {location}")
        }
        _ => "Internal server error".to_string(),
    };

    // TODO: Is it correct?!
    let error = ErrorResponse {
        message,
        status_code: status_code.as_u16(),
    };
    (status_code, Json(error)).into_response()
}
