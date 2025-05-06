use axum::{http, response::IntoResponse, Json};
use snafu::prelude::*;

use super::schemas::JsonResponse;
use crate::execution::error::ExecutionError;
use arrow::error::ArrowError;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum DbtError {
    #[snafu(display("Failed to decompress GZip body"))]
    GZipDecompress { source: std::io::Error },

    #[snafu(display("Failed to parse login request"))]
    LoginRequestParse { source: serde_json::Error },

    #[snafu(display("Failed to parse query body"))]
    QueryBodyParse { source: serde_json::Error },

    #[snafu(display("Missing auth token"))]
    MissingAuthToken,

    #[snafu(display("Invalid warehouse_id format"))]
    InvalidWarehouseIdFormat { source: uuid::Error },

    #[snafu(display("Missing DBT session"))]
    MissingDbtSession,

    #[snafu(display("Invalid auth data"))]
    InvalidAuthData,

    #[snafu(display("Feature not implemented"))]
    NotImplemented,

    #[snafu(display("Failed to parse row JSON"))]
    RowParse { source: serde_json::Error },

    #[snafu(display("UTF8 error: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(transparent)]
    Metastore {
        source: crate::http::metastore::error::MetastoreAPIError,
    },

    #[snafu(transparent)]
    Execution {
        source: crate::execution::error::ExecutionError,
    },
}

pub type DbtResult<T> = std::result::Result<T, DbtError>;

impl IntoResponse for DbtError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        if let Self::Execution { source } = self {
            return source.into_response();
        }
        if let Self::Metastore { source } = self {
            return source.into_response();
        }

        let status_code = match &self {
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidWarehouseIdFormat { .. } => http::StatusCode::BAD_REQUEST,
            Self::RowParse { .. }
            | Self::Utf8 { .. }
            | Self::Arrow { .. }
            | Self::Metastore { .. }
            | Self::Execution { .. }
            | Self::NotImplemented { .. } => http::StatusCode::OK,
            Self::MissingAuthToken | Self::MissingDbtSession | Self::InvalidAuthData => {
                http::StatusCode::UNAUTHORIZED
            }
        };

        let message = match &self {
            Self::GZipDecompress { source } => format!("failed to decompress GZip body: {source}"),
            Self::LoginRequestParse { source } => {
                format!("failed to parse login request: {source}")
            }
            Self::QueryBodyParse { source } => format!("failed to parse query body: {source}"),
            Self::InvalidWarehouseIdFormat { source } => format!("invalid warehouse_id: {source}"),
            Self::RowParse { source } => format!("failed to parse row JSON: {source}"),
            Self::MissingAuthToken | Self::MissingDbtSession | Self::InvalidAuthData => {
                "session error".to_string()
            }
            Self::Utf8 { source } => {
                format!("Error encoding UTF8 string: {source}")
            }
            Self::Arrow { source } => {
                format!("Error encoding in Arrow format: {source}")
            }
            Self::NotImplemented => "feature not implemented".to_string(),
            Self::Metastore { source } => source.to_string(),
            Self::Execution { source } => source.to_string(),
        };

        let body = Json(JsonResponse {
            success: false,
            message: Some(message),
            // TODO: On error data field contains details about actual error
            // {'data': {'internalError': False, 'unredactedFromSecureObject': False, 'errorCode': '002003', 'age': 0, 'sqlState': '02000', 'queryId': '01bb407f-0002-97af-0004-d66e006a69fa', 'line': 1, 'pos': 14, 'type': 'COMPILATION'}}
            data: None,
            code: Some(status_code.as_u16().to_string()),
        });
        (status_code, body).into_response()
    }
}

impl IntoResponse for ExecutionError {
    fn into_response(self) -> axum::response::Response {
        let status_code = match &self {
            Self::RegisterUDF { .. }
            | Self::RegisterUDAF { .. }
            | Self::InvalidTableIdentifier { .. }
            | Self::InvalidSchemaIdentifier { .. }
            | Self::InvalidFilePath { .. }
            | Self::InvalidBucketIdentifier { .. }
            | Self::TableProviderNotFound { .. }
            | Self::MissingDataFusionSession { .. }
            | Self::Utf8 { .. }
            | Self::VolumeNotFound { .. }
            | Self::ObjectStore { .. }
            | Self::ObjectAlreadyExists { .. }
            | Self::UnsupportedFileFormat { .. }
            | Self::RefreshCatalogList { .. }
            | Self::UrlParse { .. }
            | Self::JobError { .. }
            | Self::UploadFailed { .. } => http::StatusCode::BAD_REQUEST,
            Self::Arrow { .. }
            | Self::S3Tables { .. }
            | Self::Iceberg { .. }
            | Self::CatalogListDowncast { .. }
            | Self::CatalogDownCast { .. }
            | Self::RegisterCatalog { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::DatabaseNotFound { .. }
            | Self::TableNotFound { .. }
            | Self::SchemaNotFound { .. }
            | Self::CatalogNotFound { .. }
            | Self::Metastore { .. }
            | Self::DataFusion { .. }
            | Self::DataFusionQuery { .. } => http::StatusCode::OK,
        };

        let message = match &self {
            Self::DataFusion { source } => format!("DataFusion error: {source}"),
            Self::DataFusionQuery { source, query } => {
                format!("DataFusion error: {source}, query: {query}")
            }
            Self::InvalidTableIdentifier { ident } => {
                format!("Invalid table identifier: {ident}")
            }
            Self::InvalidSchemaIdentifier { ident } => {
                format!("Invalid schema identifier: {ident}")
            }
            Self::InvalidFilePath { path } => format!("Invalid file path: {path}"),
            Self::InvalidBucketIdentifier { ident } => {
                format!("Invalid bucket identifier: {ident}")
            }
            Self::Arrow { source } => format!("Arrow error: {source}"),
            Self::TableProviderNotFound { table_name } => {
                format!("No Table Provider found for table: {table_name}")
            }
            Self::MissingDataFusionSession { id } => {
                format!("Missing DataFusion session for id: {id}")
            }
            Self::Utf8 { source } => format!("Error encoding UTF8 string: {source}"),
            Self::Metastore { source } => format!("Metastore error: {source}"),
            Self::DatabaseNotFound { db } => format!("Database not found: {db}"),
            Self::TableNotFound { table } => format!("Table not found: {table}"),
            Self::SchemaNotFound { schema } => format!("Schema not found: {schema}"),
            Self::VolumeNotFound { volume } => format!("Volume not found: {volume}"),
            Self::ObjectStore { source } => format!("Object store error: {source}"),
            Self::ObjectAlreadyExists { type_name, name } => {
                format!("Object of type {type_name} with name {name} already exists")
            }
            Self::UnsupportedFileFormat { format } => {
                format!("Unsupported file format {format}")
            }
            Self::RefreshCatalogList { message } => message.clone(),
            _ => "Internal server error".to_string(),
        };

        let body = Json(JsonResponse {
            success: false,
            message: Some(message),
            // TODO: On error data field contains details about actual error
            // {'data': {'internalError': False, 'unredactedFromSecureObject': False, 'errorCode': '002003', 'age': 0, 'sqlState': '02000', 'queryId': '01bb407f-0002-97af-0004-d66e006a69fa', 'line': 1, 'pos': 14, 'type': 'COMPILATION'}}
            data: None,
            code: Some(status_code.as_u16().to_string()),
        });
        (status_code, body).into_response()
    }
}
