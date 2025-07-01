use crate::error::{self as api_snowflake_rest_error, Result};
use crate::handlers::login;
use crate::state::AppState;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::post;
use core_executor::models::QueryContext;
use regex::Regex;

pub fn create_router() -> Router<AppState> {
    Router::new().route("/session/v1/login-request", post(login))
}

#[allow(clippy::unwrap_used)]
pub async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<impl IntoResponse> {
    // no demo user -> no auth required
    if state.config.auth.demo_user.is_empty() || state.config.auth.demo_password.is_empty() {
        return Ok(next.run(req).await);
    }

    let Some(token) = extract_token(req.headers()) else {
        return api_snowflake_rest_error::MissingAuthTokenSnafu.fail();
    };

    //TODO: check if session exists
    if state
        .execution_svc
        .query(token.as_str(), "SELECT 1", QueryContext::default())
        .await
        .is_err()
    {
        return api_snowflake_rest_error::InvalidAuthTokenSnafu.fail();
    }

    Ok(next.run(req).await)
}

#[must_use]
pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    headers.get("authorization").and_then(|value| {
        value.to_str().ok().and_then(|auth| {
            #[allow(clippy::unwrap_used)]
            let re = Regex::new(r#"Snowflake Token="([a-f0-9\-]+)""#).unwrap();
            re.captures(auth)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        })
    })
}
