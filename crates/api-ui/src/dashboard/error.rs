use crate::error::ErrorResponse;
use crate::error::IntoStatusCode;
use crate::queries::error::QueryError;
use axum::Json;
use axum::response::IntoResponse;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

pub type DashboardResult<T> = Result<T, DashboardAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DashboardAPIError {
    #[snafu(display("Get total: {source}"))]
    Metastore { source: MetastoreError },
    #[snafu(display("Get total: {source}"))]
    Queries { source: QueryError },
    #[snafu(display("Get total: {source}"))]
    History {
        source: core_history::errors::HistoryStoreError,
    },
}

// Select which status code to return.
impl IntoStatusCode for DashboardAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Metastore { .. } | Self::Queries { .. } | Self::History { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

// generic
impl IntoResponse for DashboardAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
