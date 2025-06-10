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

    let message = match error {
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
        } => {
            format!(
                "Object of type {type_name} with name {name} already exists, location: {location}"
            )
        }
        ExecutionError::UnsupportedFileFormat { format, location } => {
            format!("Unsupported file format {format}, location: {location}")
        }
        ExecutionError::RefreshCatalogList { source, location } => {
            format!("Refresh catalog list error: {source}, location: {location}")
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
