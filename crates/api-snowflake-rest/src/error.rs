use crate::models::JsonResponse;
use crate::models::ResponseData;
use axum::{Json, http, response::IntoResponse};
use core_executor::status_code::StatusCode;
use datafusion::arrow::error::ArrowError;
use error_stack::ErrorChainExt;
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
        fields(
            query_id,
            display_error,
            debug_error,
            error_stack_trace,
            error_chain,
            status_code
        ),
        skip(self)
    )]
    #[allow(clippy::too_many_lines)]
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        // TODO: Here we have different status codes for different errors
        // - first, there is http status code
        // - second, there is snowflake error status code
        // - third, there is snowflake error sqlState
        // For a very specific error we need to be able to match all three, message is much less relevant

        // SQLSTATE - consists of 5 bytes. They are divided into two parts: the first and second bytes contain a class and the following three a subclass.
        // Each class belongs to one of four categories: "S" denotes "Success" (class 00), "W" denotes "Warning" (class 01), "N" denotes "No data" (class 02),
        // and "X" denotes "Exception" (all other classes).
        let (http_code, sql_state, status_code) = match &self {
            Self::Execution { source } => match source.to_snowflake_error().status_code() {
                StatusCode::Internal => (
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    "02000",
                    StatusCode::Internal,
                ),
                StatusCode::ObjectStore => (
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    "02000",
                    StatusCode::ObjectStore,
                ),
                StatusCode::NotFound => (http::StatusCode::OK, "02000", StatusCode::NotFound),
                _ => (http::StatusCode::OK, "02000", StatusCode::Other),
            },
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidWarehouseIdFormat { .. } => {
                (http::StatusCode::BAD_REQUEST, "02000", StatusCode::Other)
            }
            Self::MissingAuthToken { .. }
            | Self::MissingDbtSession { .. }
            | Self::InvalidAuthData { .. }
            | Self::InvalidAuthToken { .. } => {
                (http::StatusCode::UNAUTHORIZED, "02000", StatusCode::Other)
            }
            Self::RowParse { .. }
            | Self::Utf8 { .. }
            | Self::Arrow { .. }
            | Self::NotImplemented { .. } => (http::StatusCode::OK, "02000", StatusCode::Other),
        };

        let display_error = self.display_error_message();
        // Give more context to user, not just "Internal server error"
        // if status_code == http::StatusCode::INTERNAL_SERVER_ERROR {
        //     display_error = "Internal server error".to_string();
        // }sno

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("status_code", status_code.to_string())
            .record("query_id", self.query_id())
            .record("display_error", &display_error)
            .record("debug_error", self.debug_error_message())
            .record("error_stack_trace", self.output_msg())
            .record("error_chain", self.error_chain());

        let body = Json(JsonResponse {
            success: false,
            message: Some(display_error),
            // TODO: On error data field contains details about actual error
            // {'data': {'internalError': False, 'unredactedFromSecureObject': False, 'errorCode': '002043', 'age': 0, 'sqlState': '02000', 'queryId': '01be8b7b-0003-6429-0004-d66e02e60096', 'line': -1, 'pos': -1, 'type': 'COMPILATION'}, 'code': '002043', 'message': 'SQL compilation error:\nObject does not exist, or operation cannot be performed.', 'success': False, 'headers': None}
            // {'data': {'rowtype': [], 'rowsetBase64': None, 'rowset': None, 'total': None, 'queryResultFormat': None, 'errorCode': '002043', 'sqlState': '02000'}, 'success': False, 'message': "8244114031572: SQL compilation error: Schema 'embucket.no' does not exist or not authorized", 'code': '002043'}
            data: Some(ResponseData {
                error_code: Some(status_code.to_string()),
                sql_state: Some(sql_state.to_string()),
                // TODO: fill in other fields, some of them shouldn't be here at all for errors
                row_type: Vec::new(),
                row_set_base_64: None,
                row_set: None,
                total: None,
                query_result_format: None,
                query_id: Some(self.query_id()),
            }),
            code: Some(status_code.to_string()),
        });
        (http_code, body).into_response()
    }
}

impl Error {
    pub fn query_id(&self) -> String {
        if let Self::Execution { source, .. } = self {
            source.query_id().as_uuid().to_string()
        } else {
            String::new()
        }
    }

    pub fn display_error_message(&self) -> String {
        if let Self::Execution { source, .. } = self {
            source.to_snowflake_error().display_error_message()
        } else {
            self.to_string()
        }
    }

    pub fn debug_error_message(&self) -> String {
        if let Self::Execution { source, .. } = self {
            source.to_snowflake_error().debug_error_message()
        } else {
            format!("{self:?}")
        }
    }
}
