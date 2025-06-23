use crate::schemas::JsonResponse;
use axum::{Json, http, response::IntoResponse};
use core_executor::error::ExecutionError;
use datafusion::arrow::error::ArrowError;
use error_stack::ErrorExt;
use error_stack_trace;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

// TBD: Why context at error/source mostly not used in error?
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
        tracing::error!("{}", self.output_msg());
        let (status_code, message) = if let Self::Execution { source } = &self {
            convert_into_status_code_and_error(source)
        } else {
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
            (status_code, self.to_string())
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
fn convert_into_status_code_and_error(error: &ExecutionError) -> (StatusCode, String) {
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
        ExecutionError::DataFusion { .. }
        | ExecutionError::DataFusionQuery { .. }
        | ExecutionError::InvalidTableIdentifier { .. }
        | ExecutionError::InvalidSchemaIdentifier { .. }
        | ExecutionError::InvalidFilePath { .. }
        | ExecutionError::InvalidBucketIdentifier { .. }
        | ExecutionError::Arrow { .. }
        | ExecutionError::TableProviderNotFound { .. }
        | ExecutionError::MissingDataFusionSession { .. }
        | ExecutionError::Utf8 { .. }
        | ExecutionError::Metastore { .. }
        | ExecutionError::DatabaseNotFound { .. }
        | ExecutionError::TableNotFound { .. }
        | ExecutionError::SchemaNotFound { .. }
        | ExecutionError::VolumeNotFound { .. }
        | ExecutionError::ObjectStore { .. }
        | ExecutionError::ObjectAlreadyExists { .. }
        | ExecutionError::UnsupportedFileFormat { .. }
        | ExecutionError::RefreshCatalogList { .. } => error.to_string(),
        _ => "Internal server error".to_string(),
    };

    (status_code, message)
}
