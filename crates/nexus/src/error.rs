use axum::{response::IntoResponse, response::Response};


impl From<control_plane::error::Error> for AppError {
    fn from(_err: control_plane::error::Error) -> Self {
        Self {} 
    }
}

impl From<catalog::error::Error> for AppError {
    fn from(_err: catalog::error::Error) -> Self {
        Self {} 
    }
}

pub struct AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error");
        (status, message).into_response()
    }
}