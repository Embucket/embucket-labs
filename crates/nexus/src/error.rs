use axum::http::StatusCode;
use axum::{response::IntoResponse, response::Response};
use catalog::error::CatalogError;
use control_plane::error::ControlPlaneError;

impl From<ControlPlaneError> for AppError {
    fn from(err: ControlPlaneError) -> Self {
        Self {
            message: err.to_string(),
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Map according to spec <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
impl From<CatalogError> for AppError {
    fn from(err: CatalogError) -> Self {
        let status = match err {
            CatalogError::DatabaseNotFound { .. } | CatalogError::TableNotFound { .. } => {
                StatusCode::NOT_FOUND
            }
            CatalogError::NamespaceAlreadyExists { .. }
            | CatalogError::TableAlreadyExists { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            message: err.to_string(),
            status_code: status,
        }
    }
}

#[derive(Debug)]
pub struct AppError {
    pub message: String,
    pub status_code: StatusCode,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let message = (if self.message.is_empty() {
            "Internal Server Error".to_string()
        } else {
            self.message
        },);
        (self.status_code, message).into_response()
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppError(\"{}\")", self.message)
    }
}
