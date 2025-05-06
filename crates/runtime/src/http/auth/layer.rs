use super::error::{AuthError, AuthResult, BadAuthTokenSnafu};
use super::handlers::get_claims_validate_jwt_token;
use crate::http::state::AppState;
use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use http::HeaderMap;
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;

fn get_authorization_token(headers: &HeaderMap) -> AuthResult<String> {
    let auth = headers.get(http::header::AUTHORIZATION);

    match auth {
        Some(auth_header) => {
            if let Ok(auth_header_str) = auth_header.to_str() {
                match auth_header_str.strip_prefix("Bearer ") {
                    Some(token) => Ok(token.to_string()),
                    None => Err(AuthError::BadAuthHeader),
                }
            } else {
                Err(AuthError::BadAuthHeader)
            }
        }
        None => Err(AuthError::NoAuthHeader),
    }
}

pub async fn require_auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> AuthResult<impl IntoResponse> {
    // no demo user -> no auth required
    if state.auth_config.demo_user().is_empty() {
        return Ok(next.run(req).await);
    }

    let access_token = get_authorization_token(&req.headers())?;

    let audience = &state.config.host;
    let jwt_secret = state.auth_config.jwt_secret();

    if let Err(_) = get_claims_validate_jwt_token(&access_token, audience, jwt_secret) {
        return Err(AuthError::BadAuthToken);
    }

    Ok(next.run(req).await)
}
