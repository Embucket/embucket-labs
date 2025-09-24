use crate::SqlState;
use crate::models::JsonResponse;
use crate::models::ResponseData;
use axum::{Json, http, response::IntoResponse};
use core_executor::error::OperationOn;
use core_executor::error_code::ErrorCode;
use core_executor::snowflake_error::Entity;
use core_history::QueryRecordId;
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

    #[snafu(display("Invalid uuid format"))]
    InvalidUuidFormat {
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
        let (http_code, body) = self.prepare_response();
        (http_code, body).into_response()
    }
}

impl Error {
    pub fn query_id(&self) -> QueryRecordId {
        if let Self::Execution { source, .. } = self {
            source.query_id()
        } else {
            QueryRecordId::default()
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
    #[tracing::instrument(
        name = "api_snowflake_rest::Error::prepare_response",
        level = "info",
        fields(
            query_id,
            display_error,
            debug_error,
            error_stack_trace,
            error_chain,
            error_code,
            sql_state,
        ),
        skip(self)
    )]
    #[allow(clippy::too_many_lines)]
    pub fn prepare_response(&self) -> (http::StatusCode, Json<JsonResponse>) {
        // TODO: Here we have different status codes for different errors
        // - first, there is http status code
        // - second, there is snowflake error sqlState
        // - third, there is snowflake error error code
        // For a very specific error we need to be able to match all three, message is much less relevant

        let (http_code, sql_state, error_code) = match &self {
            Self::Execution { source } => {
                let error_code = source.to_snowflake_error().error_code();
                match error_code {
                    ErrorCode::Internal => (
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        SqlState::Success,
                        error_code,
                    ),
                    ErrorCode::ObjectStore => (
                        http::StatusCode::SERVICE_UNAVAILABLE,
                        SqlState::Success,
                        error_code,
                    ),
                    ErrorCode::HistoricalQueryError => (
                        http::StatusCode::OK,
                        SqlState::GenericQueryErrorFromHistory,
                        error_code,
                    ),
                    ErrorCode::EntityNotFound(entity_type, operation_on) => {
                        match (entity_type, operation_on) {
                            // table not found
                            (Entity::Table, OperationOn::Table(..)) => {
                                (http::StatusCode::OK, SqlState::DoesNotExist, error_code)
                            }
                            _ => (http::StatusCode::OK, SqlState::Success, error_code),
                        }
                    }
                    ErrorCode::DataFusionSql | ErrorCode::DataFusionSqlParse => {
                        (http::StatusCode::OK, SqlState::Success, error_code)
                    }
                    _ => (http::StatusCode::OK, SqlState::Success, error_code),
                }
            }
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidUuidFormat { .. } => {
                // TODO: Is this need a fix? Bad request return retriable http code
                (
                    http::StatusCode::BAD_REQUEST,
                    SqlState::Success,
                    ErrorCode::Other,
                )
            }
            Self::MissingAuthToken { .. }
            | Self::MissingDbtSession { .. }
            | Self::InvalidAuthData { .. }
            | Self::InvalidAuthToken { .. } => (
                http::StatusCode::UNAUTHORIZED,
                SqlState::Success,
                ErrorCode::Other,
            ),
            Self::RowParse { .. }
            | Self::Utf8 { .. }
            | Self::Arrow { .. }
            | Self::NotImplemented { .. } => {
                (http::StatusCode::OK, SqlState::Success, ErrorCode::Other)
            }
        };

        // Give more context to user, not just "Internal server error"
        let display_error = self.display_error_message();

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("error_code", error_code.to_string())
            .record("sql_state", sql_state.to_string())
            .record("query_id", self.query_id().as_i64())
            .record("query_uuid", self.query_id().as_uuid().to_string())
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
                error_code: Some(error_code.to_string()),
                sql_state: Some(sql_state.to_string()),
                // TODO: fill in other fields, some of them shouldn't be here at all for errors
                row_type: Vec::new(),
                row_set_base_64: None,
                row_set: None,
                total: None,
                query_result_format: None,
                // Query uuid is returned to the user
                query_id: Some(self.query_id().as_uuid().to_string()),
            }),
            code: Some(error_code.to_string()),
        });
        (http_code, body)
    }
}
