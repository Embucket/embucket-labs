use axum::{response::IntoResponse, response::Response};

impl From<control_plane::error::Error> for AppError {
    fn from(_err: control_plane::error::Error) -> Self {
        Self {
            message: _err.to_string(),
        }
    }
}

impl From<catalog::error::Error> for AppError {
    fn from(_err: catalog::error::Error) -> Self {
        Self {
            message: _err.to_string(),
        }
    }
}

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
                self.message.clone()
            },
        );
        (status, message).into_response()
    }
}
