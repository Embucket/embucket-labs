use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::state::State as AppState;

/// Middleware to validate Bearer token authorization for Iceberg REST API
///
/// If a bearer token is configured in the state, validates incoming requests
/// against it. Requests without proper authorization return 401 Unauthorized.
/// If no bearer token is configured, all requests are allowed through.
pub async fn require_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    // If no bearer token is configured, allow all requests
    let Some(configured_token) = &state.bearer_token else {
        return next.run(req).await;
    };

    // Extract the bearer token from the Authorization header
    let auth_header = req.headers().get(axum::http::header::AUTHORIZATION);

    let provided_token = if let Some(auth_header) = auth_header {
        if let Ok(auth_str) = auth_header.to_str() {
            auth_str.strip_prefix("Bearer ").map(|s| s.to_string())
        } else {
            None
        }
    } else {
        None
    };

    match provided_token {
        Some(token) if &token == configured_token => next.run(req).await,
        Some(_) => (StatusCode::UNAUTHORIZED, "Invalid bearer token").into_response(),
        None => (StatusCode::UNAUTHORIZED, "Missing Authorization header").into_response(),
    }
}
