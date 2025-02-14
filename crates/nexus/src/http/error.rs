use axum::response::IntoResponse;
use http::header::InvalidHeaderValue;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum NexusHttpError {
    #[snafu(display("Error parsing Allow-Origin header: {}", source))]
    AllowOriginHeaderParse { source: InvalidHeaderValue },

    #[snafu(display("Session load error: {msg}"))]
    SessionLoad { msg: String },
    #[snafu(display("Unable to persist session"))]
    SessionPersist {
        source: tower_sessions::session::Error,
    },
}

impl IntoResponse for NexusHttpError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::AllowOriginHeaderParse { .. } => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Allow-Origin header parse error",
            )
                .into_response(),
            Self::SessionLoad { .. } => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Session load error",
            )
                .into_response(),
            Self::SessionPersist { .. } => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Session persist error",
            )
                .into_response(),
        }
    }
}
