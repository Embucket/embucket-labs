use axum::Json;
use axum::{response::IntoResponse};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(transparent)]
    Auth { source: crate::auth::Error },
    #[snafu(transparent)]
    Dashboard { source: crate::dashboard::Error },
    #[snafu(transparent)]
    Databases { source: crate::databases::Error },
    #[snafu(transparent)]
    NavigationTrees { source: crate::navigation_trees::Error },
    #[snafu(transparent)]
    QueriesError { source: crate::queries::Error },
    #[snafu(transparent)]
    Schemas { source: crate::schemas::Error },
    #[snafu(transparent)]
    Tables { source: crate::tables::Error },
    #[snafu(transparent)]
    Volumes { source: crate::volumes::Error },
    #[snafu(transparent)]
    WebAssets { source: crate::web_assets::Error },
    #[snafu(transparent)]
    Worksheets { source: crate::worksheets::Error },
}

pub(crate) trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Auth { source } => source.status_code(),
            Self::Dashboard { source } => source.status_code(),
            Self::Databases { source } => source.status_code(),
            Self::NavigationTrees { source } => source.status_code(),
            Self::QueriesError { source } => source.status_code(),
            Self::Schemas { source } => source.status_code(),
            Self::Tables { source } => source.status_code(),
            Self::Volumes { source } => source.status_code(),
            Self::WebAssets { source } => source.status_code(),
            Self::Worksheets { source } => source.status_code(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        match self {
            Self::Auth { source, .. } => source.into_response(),
            _ => (code, Json(error)).into_response(),
        }
    }
}
