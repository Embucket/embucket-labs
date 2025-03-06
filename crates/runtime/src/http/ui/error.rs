use axum::response::IntoResponse;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum UIError {
    #[snafu(transparent)]
    Execution {
        source: crate::execution::error::ExecutionError,
    },
}
pub type UIResult<T> = Result<T, UIError>;

impl IntoResponse for UIError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        match self {
            UIError::Execution { source } => source.into_response(),
        }
    }
}
