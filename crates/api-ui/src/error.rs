use axum::response::{IntoResponse, Response};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum UIError {
    #[snafu(transparent)]
    Execution {
        source: core_executor::error::ExecutionError,
    },
    #[snafu(transparent)]
    Metastore {
        source: core_metastore::error::MetastoreError,
    },
}
pub type UIResult<T> = Result<T, UIError>;

pub(crate) trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub(crate) struct UIResponse<T> {
//     #[serde(flatten)]
//     pub(crate) data: T,
// }
//
// impl<T> UIResponse<T> {
//     pub const fn from(data: T) -> Json<Self> {
//         Json(Self { data })
//     }
// }

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoResponse for UIError {
    fn into_response(self) -> Response<axum::body::Body> {
        match self {
            Self::Execution { source } => source.into_response(),
            Self::Metastore { source } => source.into_response(),
        }
    }
}
