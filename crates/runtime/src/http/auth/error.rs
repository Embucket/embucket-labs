use super::models::WwwAuthenticate;
use crate::http::error::ErrorResponse;
use axum::{http, response::IntoResponse, Json};
use http::header;
use http::header::InvalidHeaderValue;
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
    #[snafu(display("Bad refresh token. {source}"))]
    BadRefreshToken { source: JwtError },

    #[snafu(display("Bad authentication token. {source}"))]
    BadAuthToken { source: JwtError },

    #[snafu(display("Bad Authorization header"))]
    BadAuthHeader,

    #[snafu(display("No Authorization header"))]
    NoAuthHeader,

    #[snafu(display("No refresh_token cookie"))]
    NoRefreshTokenCookie,

    // programmatic errors goes here:

    #[snafu(display("Can't add header to response: {source}"))]
    ResponseHeader { source: InvalidHeaderValue },

    #[snafu(display("Set-Cookie error: {source}"))]
    SetCookie { source: MaxSizeReached },

    #[snafu(display("JWT create error: {source}"))]
    CreateJwt { source: JwtError },

    #[cfg(test)]
    #[snafu(display("Custom error: {message}"))]
    Custom { message: String },
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        let (status, www_value) = match self {
            Self::Login => {
                (StatusCode::UNAUTHORIZED, Some(WwwAuthenticate {
                    auth: "Basic".to_string(),
                    realm: "login".to_string(),
                    error: "Login error".to_string(),
                }))
            }
            Self::NoAuthHeader
            | Self::NoRefreshTokenCookie
            | Self::BadRefreshToken { .. }
            | Self::BadAuthToken { .. } => {
                // reuse error message
                (StatusCode::UNAUTHORIZED, Some(WwwAuthenticate {
                    auth: "Bearer".to_string(),
                    realm: "api-auth".to_string(),
                    error: self.to_string(),
                }))
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, None),
        };

        let body = Json(ErrorResponse {
            message: self.to_string(),
            status_code: status.as_u16(),
        });

        match www_value {
            Some(www_value) => (
                StatusCode::UNAUTHORIZED,
                // rfc7235
                [(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_str(&www_value.to_string())
                        // Not sure if this error can ever happen, but in any case
                        // we have no options as already handling error response
                        .unwrap_or_else(|_| {
                            HeaderValue::from_static("Error adding www_authenticate to HeaderValue")
                        }),
                )],
                body,
            ).into_response(),
            None => (status, body).into_response(),
        }
    }
}

//  pub type AuthResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub type AuthResult<T> = std::result::Result<T, AuthError>;
