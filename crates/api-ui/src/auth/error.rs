use crate::error::IntoStatusCode;
use axum::{Json, http, response::IntoResponse};
use error_stack_trace;
use http::HeaderValue;
use http::header;
use http::header::InvalidHeaderValue;
use http::{StatusCode, header::MaxSizeReached};
use jsonwebtoken::errors::{Error as JwtError, ErrorKind as JwtErrorKind};
use serde::{Deserialize, Serialize};
use snafu::Location;
use snafu::prelude::*;
use utoipa::ToSchema;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Login error"))]
    Login {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No JWT secret set"))]
    NoJwtSecret {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Bad refresh token. {error}"))]
    BadRefreshToken {
        #[snafu(source)]
        error: JwtError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Bad authentication token. {error}"))]
    BadAuthToken {
        #[snafu(source)]
        error: JwtError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Bad Authorization header"))]
    BadAuthHeader {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No Authorization header"))]
    NoAuthHeader {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No refresh_token cookie"))]
    NoRefreshTokenCookie {
        #[snafu(implicit)]
        location: Location,
    },

    // programmatic errors goes here:
    #[snafu(display("Can't add header to response: {error}"))]
    ResponseHeader {
        #[snafu(source)]
        error: InvalidHeaderValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Set-Cookie error: {error}"))]
    SetCookie {
        #[snafu(source)]
        error: MaxSizeReached,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("JWT create error: {error}"))]
    CreateJwt {
        #[snafu(source)]
        error: JwtError,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(test)]
    #[snafu(display("Custom error: {message}"))]
    Custom {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TokenErrorKind {
    InvalidToken,
    InvalidSignature,
    InvalidEcdsaKey,
    InvalidRsaKey,
    RsaFailedSigning,
    InvalidAlgorithmName,
    InvalidKeyFormat,

    // validation errors
    MissingRequiredClaim,
    ExpiredSignature,
    InvalidIssuer,
    InvalidAudience,
    InvalidSubject,
    ImmatureSignature,
    InvalidAlgorithm,
    MissingAlgorithm,

    Other,
}

impl From<JwtErrorKind> for TokenErrorKind {
    fn from(value: JwtErrorKind) -> Self {
        match value {
            JwtErrorKind::InvalidToken => Self::InvalidToken,
            JwtErrorKind::InvalidSignature => Self::InvalidSignature,
            JwtErrorKind::InvalidEcdsaKey => Self::InvalidEcdsaKey,
            JwtErrorKind::InvalidRsaKey(_) => Self::InvalidRsaKey,
            JwtErrorKind::RsaFailedSigning => Self::RsaFailedSigning,
            JwtErrorKind::InvalidAlgorithmName => Self::InvalidAlgorithmName,
            JwtErrorKind::InvalidKeyFormat => Self::InvalidKeyFormat,
            JwtErrorKind::MissingRequiredClaim(_) => Self::MissingRequiredClaim,
            JwtErrorKind::ExpiredSignature => Self::ExpiredSignature,
            JwtErrorKind::InvalidIssuer => Self::InvalidIssuer,
            JwtErrorKind::InvalidAudience => Self::InvalidAudience,
            JwtErrorKind::InvalidSubject => Self::InvalidSubject,
            JwtErrorKind::ImmatureSignature => Self::ImmatureSignature,
            JwtErrorKind::InvalidAlgorithm => Self::InvalidAlgorithm,
            JwtErrorKind::MissingAlgorithm => Self::MissingAlgorithm,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthErrorResponse {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<TokenErrorKind>,
    pub status_code: u16,
}

// WwwAuthenticate is error related so placed closer to error
// Return WwwAuthenticate header along with Unauthorized status code
#[cfg_attr(test, derive(Debug))]
pub struct WwwAuthenticate {
    pub auth: String,
    pub realm: String,
    pub error: String,
    pub kind: Option<TokenErrorKind>,
}

impl TryFrom<Error> for WwwAuthenticate {
    type Error = Option<Self>;
    fn try_from(value: Error) -> std::result::Result<Self, Self::Error> {
        let auth = "Bearer".to_string();
        let error = value.to_string();
        match value {
            Error::Login { .. } => Ok(Self {
                auth,
                realm: "login".to_string(),
                error,
                kind: None,
            }),
            Error::NoAuthHeader { .. } | Error::NoRefreshTokenCookie { .. } => Ok(Self {
                auth,
                realm: "api-auth".to_string(),
                error,
                kind: None,
            }),
            Error::BadRefreshToken { error: source, .. }
            | Error::BadAuthToken { error: source, .. } => Ok(Self {
                auth,
                realm: "api-auth".to_string(),
                error,
                kind: Some(TokenErrorKind::from(source.kind().clone())),
            }),
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
            Some(kind) => write!(f, r#"{base}, kind="{kind:?}""#),
            None => write!(f, "{base}"),
        }
    }
}

impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Login { .. }
            | Self::NoAuthHeader { .. }
            | Self::NoRefreshTokenCookie { .. }
            | Self::BadRefreshToken { .. }
            | Self::BadAuthToken { .. } => StatusCode::UNAUTHORIZED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        let message = self.to_string();
        let status_code = self.status_code();
        let www_authenticate: std::result::Result<WwwAuthenticate, Option<WwwAuthenticate>> =
            self.try_into();

        match www_authenticate {
            Ok(www_value) => (
                status_code,
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
                Json(AuthErrorResponse {
                    message,
                    error_kind: www_value.kind,
                    status_code: status_code.as_u16(),
                }),
            )
                .into_response(),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthErrorResponse {
                    message,
                    error_kind: None,
                    status_code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                }),
            )
                .into_response(),
        }
    }
}
