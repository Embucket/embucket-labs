use axum::extract::multipart::MultipartError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use catalog::error::Error as CatalogError;
use control_plane::error::Error as ControlError;
use utoipa::ToSchema;

#[derive(thiserror::Error, Debug, ToSchema)]
pub enum AppError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("internal server error: {0}")]
    InternalServerError(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("not empty: {0}")]
    NotEmpty(String),
    #[error("unprocessable entity: {0}")]
    UnprocessableEntity(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("DB error: {0}")]
    DbError(String),
    #[error("Iceberg error: {0}")]
    IcebergError(String),
    #[error("invalid credentials error: {0}")]
    InvalidCredentials(String),
}

impl AppError {
    pub fn new<T: Into<AppError>>(err: T, message: &str) -> Self {
        let mut error = err.into();
        error.with(message);
        error
    }

    pub fn with(&mut self, new_message: &str) {
        match self {
            AppError::BadRequest(ref mut msg) => *msg = new_message.to_string(),
            AppError::NotFound(ref mut msg) => *msg = new_message.to_string(),
            AppError::InternalServerError(ref mut msg) => *msg = new_message.to_string(),
            AppError::AlreadyExists(ref mut msg) => *msg = new_message.to_string(),
            AppError::NotEmpty(ref mut msg) => *msg = new_message.to_string(),
            AppError::UnprocessableEntity(ref mut msg) => *msg = new_message.to_string(),
            AppError::NotImplemented(ref mut msg) => *msg = new_message.to_string(),
            AppError::DbError(ref mut msg) => *msg = new_message.to_string(),
            AppError::IcebergError(ref mut msg) => *msg = new_message.to_string(),
            AppError::InvalidCredentials(ref mut msg) => *msg = new_message.to_string(),
        }
    }
}
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::NotFound(ref _e) => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::DbError(ref _e) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            AppError::InternalServerError(ref _e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
            AppError::InvalidCredentials(ref _e) => {
                (StatusCode::UNPROCESSABLE_ENTITY, self.to_string())
            }
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::UnprocessableEntity(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        (status, error_message).into_response()
    }
}
impl From<ControlError> for AppError {
    fn from(e: ControlError) -> Self {
        match e {
            ControlError::NotEmpty(e) => AppError::AlreadyExists(e.to_string()),
            ControlError::InvalidInput(e) => AppError::BadRequest(e.to_string()),
            ControlError::ErrNotFound => AppError::NotFound(e.to_string()),
            ControlError::InvalidCredentials(e) => AppError::InvalidCredentials(e.to_string()),
            ControlError::DataFusionError(e) => AppError::UnprocessableEntity(e.to_string()),
            ControlError::IceLakeError(e) => AppError::UnprocessableEntity(e.to_string()),
        }
    }
}
impl From<CatalogError> for AppError {
    fn from(e: CatalogError) -> Self {
        match e {
            CatalogError::ErrNotFound => AppError::NotFound(e.to_string()),
            CatalogError::ErrAlreadyExists => AppError::AlreadyExists(e.to_string()),
            CatalogError::NotEmpty(e) => AppError::NotEmpty(e.to_string()),
            CatalogError::NotImplemented => AppError::NotImplemented(e.to_string()),
            CatalogError::InvalidInput(e) => AppError::BadRequest(e.to_string()),
            CatalogError::FailedRequirement(e) => AppError::UnprocessableEntity(e.to_string()),
            CatalogError::DbError(e) => AppError::DbError(e.to_string()),
            CatalogError::IcebergError(e) => AppError::IcebergError(e.to_string()),
            _ => AppError::InternalServerError(e.to_string()),
        }
    }
}

impl From<MultipartError> for AppError {
    fn from(e: MultipartError) -> Self {
        AppError::InternalServerError(e.to_string())
    }
}
