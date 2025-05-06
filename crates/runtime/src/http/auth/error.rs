use super::models::WwwAuthenticate;
use crate::http::error::ErrorResponse;
use axum::{http, response::IntoResponse, Json};
use embucket_history::auth_store::AuthStoreError;
use http::header;
use http::HeaderValue;
use http::{header::MaxSizeReached, StatusCode};
use jsonwebtoken::errors::Error as JwtError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum AuthError {
    #[snafu(display("Login error"))]
    Login,

    #[snafu(display("No JWT secret set"))]
    NoJwtSecret,

    // Do not output sensitive error details
    #[snafu(display("Bad refresh token"))]
    BadRefreshToken { source: JwtError }, // keep source for snafu selector

    #[snafu(display("Bad authentication token"))]
    BadAuthToken,

    #[snafu(display("Response Header error: {source}"))]
    ResponseHeader { source: MaxSizeReached },

    #[snafu(display("Missing refresh_token cookie"))]
    NoRefreshTokenCookie,

    #[snafu(display("Missing authorization header"))]
    NoAuthHeader,

    #[snafu(display("Bad Authorization header"))]
    BadAuthHeader,

    #[snafu(display("JWT create error: {source}"))]
    CreateJwt { source: JwtError },

    #[cfg(test)]
    #[snafu(display("Custom error: {message}"))]
    Custom { message: String },
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        let auth = "Bearer".to_string();
        let mut realm = "api-auth".to_string();
        let mut error = self.to_string();

        let status = match self {
            AuthError::Login => {
                error = "Login error".to_string();
                realm = "login".to_string();
                StatusCode::UNAUTHORIZED
            }
            AuthError::NoAuthHeader | AuthError::BadAuthToken => {
                error = "The authorization token is missing or invalid".to_string();
                StatusCode::UNAUTHORIZED
            }
            AuthError::NoRefreshTokenCookie | AuthError::BadRefreshToken { .. } => {
                error = "The refresh token is missing or invalid".to_string();
                StatusCode::UNAUTHORIZED
            }
            AuthError::NoJwtSecret
            | AuthError::CreateJwt { .. } | _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(ErrorResponse {
            message: self.to_string(),
            status_code: status.as_u16(),
        });
        if status == StatusCode::UNAUTHORIZED {
            // rfc7235
            let www_value = WwwAuthenticate { auth, realm, error };
            (
                StatusCode::UNAUTHORIZED,
                [(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_str(&www_value.to_string()).unwrap(),
                )],
                body,
            )
                .into_response()
        } else {
            (status, body).into_response()
        }
    }
}

//  pub type AuthResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub type AuthResult<T> = std::result::Result<T, AuthError>;
