use crate::error::{self as api_snowflake_rest_error, Result};
use crate::handlers::login;
use crate::state::AppState;
use api_sessions::session::extract_token;
use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::post;

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
    if state.execution_svc.get_session(token).await.is_none() {
        return api_snowflake_rest_error::InvalidAuthTokenSnafu.fail();
    }

    Ok(next.run(req).await)
}
