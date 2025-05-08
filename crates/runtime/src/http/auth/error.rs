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

// WwwAuthenticate is error related so placed closer to error
// Return WwwAuthenticate header along with Unauthorized status code
#[cfg_attr(test, derive(Debug))]
pub struct WwwAuthenticate {
    pub auth: String,
    pub realm: String,
    pub error: String,
    pub kind: Option<String>,
}

impl TryFrom<AuthError> for WwwAuthenticate {
    type Error = Option<Self>;
    fn try_from(value: AuthError) -> Result<Self, Self::Error> {
        let error = value.to_string();
        match value {
            AuthError::Login => Ok(Self {
                auth: "Basic".to_string(),
                realm: "login".to_string(),
                error,
                kind: None,
            }),
            AuthError::NoAuthHeader | AuthError::NoRefreshTokenCookie => Ok(Self {
                auth: "Bearer".to_string(),
                realm: "api-auth".to_string(),
                error,
                kind: None,
            }),
            AuthError::BadRefreshToken { source } | AuthError::BadAuthToken { source } => {
                Ok(Self {
                    auth: "Bearer".to_string(),
                    realm: "api-auth".to_string(),
                    error,
                    kind: Some(source.to_string()),
                })
            }
            _ => Err(None),
        }
    }
}

impl std::fmt::Display for WwwAuthenticate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let Self {
            auth,
            realm,
            error,
            kind,
        } = self;
        let base: String = format!(r#"{auth} realm="{realm}", error="{error}""#);
        match kind {
            Some(kind) => write!(f, r#"{base}, kind="{kind}""#),
            None => write!(f, "{base}"),
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        let message = self.to_string();
        let www_authenticate: Result<WwwAuthenticate, Option<WwwAuthenticate>> = self.try_into();

        match www_authenticate {
            Ok(www_value) => (
                StatusCode::UNAUTHORIZED,
                // rfc7235
                [(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_str(&www_value.to_string())
                        // Not sure if this error can ever happen, but in any case
                        // we have no options as already handling error response
                        .unwrap_or_else(|_| {
                            HeaderValue::from_static("Error adding www_authenticate header")
                        }),
                )],
                Json(ErrorResponse {
                    message,
                    status_code: StatusCode::UNAUTHORIZED.as_u16(),
                }),
            )
                .into_response(),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message,
                    status_code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                }),
            )
                .into_response(),
        }
    }
}

//  pub type AuthResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub type AuthResult<T> = std::result::Result<T, AuthError>;
