use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_history::Error as HistoryStoreError;
use http::status::StatusCode;
use snafu::Location;
use snafu::prelude::*;

pub type WorksheetsResult<T> = Result<T, WorksheetsAPIError>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum WorksheetsAPIError {
    #[snafu(display("Create worksheet error: {source}"))]
    Create {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get worksheet error: {source}"))]
    Get {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete worksheet error: {source}"))]
    Delete {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update worksheet error: {source}"))]
    Update {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get worksheets error: {source}"))]
    List {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
}

// Kind of reusable worksheet error
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum WorksheetError {
    #[snafu(display("HistoryStore error: {source}"))]
    Store {
        source: HistoryStoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No fields to update"))]
    NothingToUpdate {
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for WorksheetsAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. }
            | Self::Get { source, .. }
            | Self::Delete { source, .. }
            | Self::Update { source, .. }
            | Self::List { source, .. } => match &source {
                WorksheetError::Store { source, .. } => match source {
                    // use `match self` to return different status_code on the same error
                    HistoryStoreError::WorksheetAdd { .. } => StatusCode::CONFLICT,
                    HistoryStoreError::BadKey { .. }
                    | HistoryStoreError::WorksheetGet { .. }
                    | HistoryStoreError::WorksheetsList { .. }
                    | HistoryStoreError::WorksheetUpdate { .. }
                    | HistoryStoreError::WorksheetDelete { .. } => StatusCode::BAD_REQUEST,
                    HistoryStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                WorksheetError::NothingToUpdate { .. } => StatusCode::BAD_REQUEST,
            },
        }
    }
}

// generic
impl IntoResponse for WorksheetsAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
