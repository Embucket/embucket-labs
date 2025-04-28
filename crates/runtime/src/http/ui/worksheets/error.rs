use crate::http::error::ErrorResponse;
use crate::http::ui::error::IntoStatusCode;
use axum::response::IntoResponse;
use axum::Json;
use embucket_history::store::WorksheetsStoreError;
use http::status::StatusCode;
use snafu::prelude::*;

pub type WorksheetsResult<T> = Result<T, WorksheetsAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum WorksheetUpdateError {
    #[snafu(transparent)]
    Store { source: WorksheetsStoreError },
    #[snafu(display("No fields to update"))]
    NothingToUpdate,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum WorksheetsAPIError {
    #[snafu(display("Create worksheet error: {source}"))]
    Create { source: WorksheetsStoreError },
    #[snafu(display("Get worksheet error: {source}"))]
    Get { source: WorksheetsStoreError },
    #[snafu(display("Delete worksheet error: {source}"))]
    Delete { source: WorksheetsStoreError },
    #[snafu(display("Update worksheet error: {source}"))]
    Update { source: WorksheetUpdateError },
    #[snafu(display("Get worksheets error: {source}"))]
    List { source: WorksheetsStoreError },
}

// Select which status code to return.
impl IntoStatusCode for WorksheetsAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                WorksheetsStoreError::WorksheetAdd { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } => match &source {
                WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                WorksheetsStoreError::BadKey { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source } => match &source {
                WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &source {
                WorksheetUpdateError::NothingToUpdate => StatusCode::BAD_REQUEST,
                WorksheetUpdateError::Store { source } => match &source {
                    WorksheetsStoreError::BadKey { .. }
                    | WorksheetsStoreError::WorksheetUpdate { .. } => StatusCode::BAD_REQUEST,
                    WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
            },
            Self::List { source } => match &source {
                WorksheetsStoreError::WorksheetsList { .. } => StatusCode::BAD_REQUEST,
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
