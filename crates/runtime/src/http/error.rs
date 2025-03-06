use axum::{response::IntoResponse, response::Response};
use serde::Serialize;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RuntimeHttpError {
    #[snafu(transparent)]
    Metastore {
        source: crate::http::metastore::error::MetastoreAPIError,
    },
    #[snafu(transparent)]
    Dbt {
        source: crate::http::dbt::error::DbtError,
    },
    #[snafu(transparent)]
    UI {
        source: crate::http::ui::error::UIError,
    },
}

impl IntoResponse for RuntimeHttpError {
    fn into_response(self) -> Response {
        match self {
            RuntimeHttpError::Metastore { source } => source.into_response(),
            RuntimeHttpError::Dbt { source } => source.into_response(),
            RuntimeHttpError::UI { source } => source.into_response(),
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorResponse(\"{}\")", self.message)
    }
}
