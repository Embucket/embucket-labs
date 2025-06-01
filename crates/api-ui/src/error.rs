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

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub(crate) struct UIResponse<T> {
//     #[serde(flatten)]
//     pub(crate) data: T,
// }
//
// impl<T> UIResponse<T> {
//     pub const fn from(data: T) -> Json<Self> {
//         Json(Self { data })
//     }
// }

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
        | MetastoreError::VolumeMissingCredentials
        | MetastoreError::Validation { .. } => http::StatusCode::BAD_REQUEST,
        MetastoreError::CloudProviderNotImplemented { .. } => http::StatusCode::PRECONDITION_FAILED,
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
        | ExecutionError::CatalogListDowncast
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
        ExecutionError::DataFusion { source } => format!("DataFusion error: {source}"),
        ExecutionError::DataFusionQuery { source, query } => {
            format!("DataFusion error: {source}, query: {query}")
        }
        ExecutionError::InvalidTableIdentifier { ident } => {
            format!("Invalid table identifier: {ident}")
        }
        ExecutionError::InvalidSchemaIdentifier { ident } => {
            format!("Invalid schema identifier: {ident}")
        }
        ExecutionError::InvalidFilePath { path } => format!("Invalid file path: {path}"),
        ExecutionError::InvalidBucketIdentifier { ident } => {
            format!("Invalid bucket identifier: {ident}")
        }
        ExecutionError::Arrow { source } => format!("Arrow error: {source}"),
        ExecutionError::TableProviderNotFound { table_name } => {
            format!("No Table Provider found for table: {table_name}")
        }
        ExecutionError::MissingDataFusionSession { id } => {
            format!("Missing DataFusion session for id: {id}")
        }
        ExecutionError::Utf8 { source } => format!("Error encoding UTF8 string: {source}"),
        ExecutionError::Metastore { source } => format!("Metastore error: {source}"),
        ExecutionError::DatabaseNotFound { db } => format!("Database not found: {db}"),
        ExecutionError::TableNotFound { table } => format!("Table not found: {table}"),
        ExecutionError::SchemaNotFound { schema } => format!("Schema not found: {schema}"),
        ExecutionError::VolumeNotFound { volume } => format!("Volume not found: {volume}"),
        ExecutionError::ObjectStore { source } => format!("Object store error: {source}"),
        ExecutionError::ObjectAlreadyExists { type_name, name } => {
            format!("Object of type {type_name} with name {name} already exists")
        }
        ExecutionError::UnsupportedFileFormat { format } => {
            format!("Unsupported file format {format}")
        }
        ExecutionError::RefreshCatalogList { source } => {
            format!("Refresh Catalog List error: {source}")
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
