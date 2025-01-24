use axum::{response::IntoResponse, response::Response};

impl From<control_plane::error::ControlPlaneError> for AppError {
    fn from(err: control_plane::error::ControlPlaneError) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

impl From<catalog::error::CatalogError> for AppError {
    fn from(err: catalog::error::CatalogError) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct AppError {
    pub message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            if self.message.is_empty() {
                "Internal Server Error".to_string()
            } else {
                self.message
            },
        );
        (status, message).into_response()
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppError(\"{}\")", self.message)
    }
}
