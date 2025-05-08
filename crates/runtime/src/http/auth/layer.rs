use super::error::{AuthError, AuthResult, BadAuthTokenSnafu};
use super::handlers::get_claims_validate_jwt_token;
use crate::http::state::AppState;
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::IntoResponse,
};
use http::HeaderMap;
use snafu::ResultExt;

fn get_authorization_token(headers: &HeaderMap) -> AuthResult<&str> {
    let auth = headers.get(http::header::AUTHORIZATION);

    match auth {
        Some(auth_header) => {
            if let Ok(auth_header_str) = auth_header.to_str() {
                match auth_header_str.strip_prefix("Bearer ") {
                    Some(token) => Ok(token),
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
    req: Request,
    next: Next,
) -> AuthResult<impl IntoResponse> {
    // no demo user -> no auth required
    if state.auth_config.jwt_secret().is_empty()
        || state.auth_config.demo_user().is_empty()
        || state.auth_config.demo_password().is_empty()
    {
        return Ok(next.run(req).await);
    }

    let access_token = get_authorization_token(req.headers())?;
    let audience = state.config.host.clone();
    let jwt_secret = state.auth_config.jwt_secret();

    let _ = get_claims_validate_jwt_token(access_token, &audience, jwt_secret)
        .context(BadAuthTokenSnafu)?;

    Ok(next.run(req).await)
}
