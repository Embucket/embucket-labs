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

    let sessions = state.execution_svc.get_sessions().await; // `get_sessions` returns an RwLock

    let sessions = sessions.read().await;

    if !sessions.contains_key(&token) {
        return api_snowflake_rest_error::InvalidAuthTokenSnafu.fail();
    }
    //Dropping the lock guard before going to the next request
    drop(sessions);

    Ok(next.run(req).await)
}
