use crate::schemas::JsonResponse;
use axum::{Json, http, response::IntoResponse};
use core_executor::SnowflakeError;
use datafusion::arrow::error::ArrowError;
use error_stack::ErrorExt;
use error_stack_trace;
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

    #[snafu(display("Invalid auth token"))]
    InvalidAuthToken {
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
    Execution { source: core_executor::Error },
}

impl IntoResponse for Error {
    #[tracing::instrument(
        name = "api_snowflake_rest::Error::into_response",
        level = "info",
        fields(display_error, debug_error, error_stack_trace, status_code),
        skip(self)
    )]
    #[allow(clippy::too_many_lines)]
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        // Record the result as part of the current span.
        tracing::Span::current().record("error_stack_trace", self.output_msg());

        let status_code = match &self {
            Self::Execution { source } => match source {
                core_executor::Error::Arrow { .. }
                | core_executor::Error::SerdeParse { .. }
                | core_executor::Error::CatalogListDowncast { .. }
                | core_executor::Error::CatalogDownCast { .. }
                | core_executor::Error::DataFusionLogicalPlanMergeTarget { .. }
                | core_executor::Error::DataFusionLogicalPlanMergeSource { .. }
                | core_executor::Error::DataFusionLogicalPlanMergeJoin { .. }
                | core_executor::Error::LogicalExtensionChildCount { .. }
                | core_executor::Error::MergeFilterStreamNotMatching { .. }
                | core_executor::Error::MatchingFilesAlreadyConsumed { .. }
                | core_executor::Error::MissingFilterPredicates { .. }
                | core_executor::Error::RegisterCatalog { .. } => {
                    http::StatusCode::INTERNAL_SERVER_ERROR
                }
                _ => http::StatusCode::OK,
            },
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidWarehouseIdFormat { .. } => http::StatusCode::BAD_REQUEST,
            Self::MissingAuthToken { .. }
            | Self::MissingDbtSession { .. }
            | Self::InvalidAuthData { .. }
            | Self::InvalidAuthToken { .. } => http::StatusCode::UNAUTHORIZED,
            Self::RowParse { .. }
            | Self::Utf8 { .. }
            | Self::Arrow { .. }
            | Self::NotImplemented { .. } => http::StatusCode::OK,
        };

        let (mut display_error, debug_error) = self.display_debug_error_messages();
        if status_code == http::StatusCode::INTERNAL_SERVER_ERROR {
            display_error = "Internal server error".to_string();
        }

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("display_error", &display_error)
            .record("debug_error", debug_error)
            .record("status_code", status_code.as_u16());

        let body = Json(JsonResponse {
            success: false,
            message: Some(display_error),
            // TODO: On error data field contains details about actual error
            // {'data': {'internalError': False, 'unredactedFromSecureObject': False, 'errorCode': '002003', 'age': 0, 'sqlState': '02000', 'queryId': '01bb407f-0002-97af-0004-d66e006a69fa', 'line': 1, 'pos': 14, 'type': 'COMPILATION'}}
            data: None,
            code: Some(status_code.as_u16().to_string()),
        });
        (status_code, body).into_response()
    }
}

impl Error {
    pub fn display_debug_error_messages(self) -> (String, String) {
        // acquire error str as later it will be moved
        if let Self::Execution { source, .. } = self {
            SnowflakeError::from(source).display_debug_error_messages()
        } else {
            (self.to_string(), format!("{self:?}"))
        }
    }
}
