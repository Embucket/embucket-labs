use crate::schemas::JsonResponse;
use axum::{Json, http, response::IntoResponse};
use core_executor::error::ExecutionError;
use datafusion::arrow::error::ArrowError;
use error_stack_trace;
use snafu::Location;
use snafu::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Failed to decompress GZip body"))]
    GZipDecompress {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse login request"))]
    LoginRequestParse {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse query body"))]
    QueryBodyParse {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing auth token"))]
    MissingAuthToken {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid warehouse_id format"))]
    InvalidWarehouseIdFormat {
        #[snafu(source)]
        error: uuid::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing DBT session"))]
    MissingDbtSession {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid auth data"))]
    InvalidAuthData {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Feature not implemented"))]
    NotImplemented {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse row JSON"))]
    RowParse {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("UTF8 error: {error}"))]
    Utf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Arrow error: {error}"))]
    Arrow {
        #[snafu(source)]
        error: ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(transparent)]
    Execution { source: ExecutionError },
}

impl IntoResponse for Error {
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
            Self::MissingAuthToken { .. }
            | Self::MissingDbtSession { .. }
            | Self::InvalidAuthData { .. } => {
                http::StatusCode::UNAUTHORIZED
            }
        };

        let message = match &self {
            Self::GZipDecompress { error, .. } => {
                format!("failed to decompress GZip body: {error}")
            }
            Self::LoginRequestParse { error, .. } => {
                format!("failed to parse login request: {error}")
            }
            Self::QueryBodyParse { error, .. } => format!("failed to parse query body: {error}"),
            Self::InvalidWarehouseIdFormat { error, .. } => {
                format!("invalid warehouse_id: {error}")
            }
            Self::RowParse { error, .. } => format!("failed to parse row JSON: {error}"),
            Self::MissingAuthToken { .. }
            | Self::MissingDbtSession { .. }
            | Self::InvalidAuthData { .. } => "session error".to_string(),
            Self::Utf8 { error, .. } => {
                format!("Error encoding UTF8 string: {error}")
            }
            Self::Arrow { error, .. } => {
                format!("Error encoding in Arrow format: {error}")
            }
            Self::NotImplemented { .. } => "feature not implemented".to_string(),
            // Self::Metastore { source } => source.to_string(),
            Self::Execution { source, .. } => source.to_string(),
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

#[allow(clippy::too_many_lines)]
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
        | ExecutionError::ObjectAlreadyExists { .. }
        | ExecutionError::UnsupportedFileFormat { .. }
        | ExecutionError::RefreshCatalogList { .. }
        | ExecutionError::UrlParse { .. }
        | ExecutionError::JobError { .. }
        | ExecutionError::OnyUseWithVariables { .. }
        | ExecutionError::OnlyPrimitiveStatements { .. }
        | ExecutionError::OnlyTableSchemaCreateStatements { .. }
        | ExecutionError::OnlyDropStatements { .. }
        | ExecutionError::OnlyDropTableViewStatements { .. }
        | ExecutionError::OnlyCreateTableStatements { .. }
        | ExecutionError::OnlyCreateStageStatements { .. }
        | ExecutionError::OnlyCopyIntoStatements { .. }
        | ExecutionError::FromObjectRequiredForCopyIntoStatements { .. }
        | ExecutionError::OnlyCreateSchemaStatements { .. }
        | ExecutionError::OnlySimpleSchemaNames { .. }
        | ExecutionError::OnlyMergeStatements { .. }
        | ExecutionError::UnsupportedShowStatement { .. }
        | ExecutionError::NoTableNamesForTruncateTable { .. }
        | ExecutionError::OnlySQLStatements { .. }
        | ExecutionError::MissingOrInvalidColumn { .. }
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
