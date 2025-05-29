use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use axum::Json;
use axum::response::IntoResponse;
use core_history::errors::HistoryStoreError;
use http::status::StatusCode;
use snafu::prelude::*;

pub type WorksheetsResult<T> = Result<T, WorksheetsAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum WorksheetUpdateError {
    #[snafu(transparent)]
    Store { source: HistoryStoreError },
    #[snafu(display("No fields to update"))]
    NothingToUpdate,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum WorksheetsAPIError {
    #[snafu(display("Create worksheet error: {source}"))]
    Create { source: HistoryStoreError },
    #[snafu(display("Get worksheet error: {source}"))]
    Get { source: HistoryStoreError },
    #[snafu(display("Delete worksheet error: {source}"))]
    Delete { source: HistoryStoreError },
    #[snafu(display("Update worksheet error: {source}"))]
    Update { source: WorksheetUpdateError },
    #[snafu(display("Get worksheets error: {source}"))]
    List { source: HistoryStoreError },
}

// Select which status code to return.
impl IntoStatusCode for WorksheetsAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                HistoryStoreError::WorksheetAdd { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } => match &source {
                HistoryStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                HistoryStoreError::BadKey { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source } => match &source {
                HistoryStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &source {
                WorksheetUpdateError::NothingToUpdate => StatusCode::BAD_REQUEST,
                WorksheetUpdateError::Store { source } => match &source {
                    HistoryStoreError::BadKey { .. }
                    | HistoryStoreError::WorksheetUpdate { .. } => StatusCode::BAD_REQUEST,
                    HistoryStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
            },
            Self::List { source } => match &source {
                HistoryStoreError::WorksheetsList { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
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
