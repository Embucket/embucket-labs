use axum::{response::IntoResponse, response::Response};
use serde::{Deserialize, Serialize};
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
            Self::Metastore { source } => source.into_response(),
            Self::Dbt { source } => source.into_response(),
            Self::UI { source } => source.into_response(),
        }
    }
}

//pub struct RuntimeHttpResult<T>(pub T);
pub type RuntimeHttpResult<T> = Result<T, RuntimeHttpError>;

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorResponse(\"{}\")", self.message)
    }
}
