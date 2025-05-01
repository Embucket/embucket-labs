use axum::{Json, http, response::IntoResponse};
use snafu::prelude::*;
use std::error::Error;
use rand::rand_core::{TryRngCore, OsRng};
use embucket_history::auth_store::AuthStoreError;
use jsonwebtoken::errors::Error as JwtError;

use crate::http::error::ErrorResponse;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum AuthError {
    #[snafu(display("Login error"))]
    Login,
    #[snafu(display("JWT error: {source}"))]
    Jwt { source: JwtError },
    #[snafu(display("Rand error: {source}"))]
    Rand { source: <OsRng as TryRngCore>::Error },
    #[snafu(display("Store error: {source}"))]
    Store { source: AuthStoreError},
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        // let status = match self {
        //     AuthError::Login | AuthError::Jwt { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
        // };
        let status = http::StatusCode::INTERNAL_SERVER_ERROR;
        let body = Json(ErrorResponse {
            message: self.to_string(),
            status_code: status.as_u16(),
        });
        (status, body).into_response()
    }
}

pub type AuthResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
