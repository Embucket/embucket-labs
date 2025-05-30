use axum::{Json, http, response::IntoResponse};
use snafu::prelude::*;

use crate::schemas::JsonResponse;
use core_executor::error::ExecutionError;
use datafusion::arrow::error::ArrowError;

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

    // #[snafu(transparent)]
    // Metastore {
    //     source: core_metastore::error::MetastoreError,
    // },
    #[snafu(transparent)]
    Execution { source: ExecutionError },
}

pub type DbtResult<T> = std::result::Result<T, DbtError>;

impl IntoResponse for DbtError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        if let Self::Execution { source } = self {
            return convert_into_response(&source);
        }
        // if let Self::Metastore { source } = self {
        //     return source.into_response();
        // }

        let status_code = match &self {
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidWarehouseIdFormat { .. } => http::StatusCode::BAD_REQUEST,
            Self::RowParse { .. }
            | Self::Utf8 { .. }
            | Self::Arrow { .. }
            // | Self::Metastore { .. }
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
            // Self::Metastore { source } => source.to_string(),
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

fn convert_into_response(error: &ExecutionError) -> axum::response::Response {
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
        | ExecutionError::UnsupportedFileFormat { .. }
        | ExecutionError::RefreshCatalogList { .. }
        | ExecutionError::UrlParse { .. }
        | ExecutionError::JobError { .. }
        | ExecutionError::UploadFailed { .. } => http::StatusCode::BAD_REQUEST,
        ExecutionError::ObjectAlreadyExists { .. } => http::StatusCode::CONFLICT,
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

    let message = match error {
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
            format!("Refresh catalog list error: {source}")
        }
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
