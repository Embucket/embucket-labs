use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_history::errors::HistoryStoreError;
use http::status::StatusCode;
use snafu::prelude::*;

pub type QueriesResult<T> = Result<T, QueriesAPIError>;

pub(crate) type QueryRecordResult<T> = Result<T, QueryError>;

// Query itself can have different kinds of errors
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum QueryError {
    #[snafu(transparent)]
    Execution {
        source: core_executor::error::ExecutionError,
    },

    #[snafu(display("History store: {source}"))]
    Store { source: HistoryStoreError },

    #[snafu(display("Failed to parse row JSON: {source}"))]
    ResultParse { source: serde_json::Error },

    #[snafu(display("ResultSet create error: {source}"))]
    CreateResultSet {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum QueriesAPIError {
    #[snafu(display("Query execution error: {source}"))]
    Query { source: QueryError },
    #[snafu(display("Error getting queries: {source}"))]
    Queries { source: QueryError },
    #[snafu(display("Error getting query record: {source}"))]
    GetQueryRecord { source: QueryError },
}

// Select which status code to return.
impl IntoStatusCode for QueriesAPIError {
    #[allow(clippy::match_wildcard_for_single_variants)]
    #[allow(clippy::collapsible_match)]
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Query { source } => match &source {
                QueryError::Execution { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                QueryError::Store { .. } => StatusCode::BAD_REQUEST,
                QueryError::ResultParse { .. }
                | QueryError::Utf8 { .. }
                | QueryError::CreateResultSet { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Queries {
                source: QueryError::ResultParse { .. },
            } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::GetQueryRecord { source } | Self::Queries { source } => match &source {
                QueryError::Store { source } => match &source {
                    HistoryStoreError::QueryGet { .. } | HistoryStoreError::BadKey { .. } => {
                        StatusCode::NOT_FOUND
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

// TODO: make it reusable by other *APIError
impl IntoResponse for QueriesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
