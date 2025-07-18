use crate::state::AppState;
use api_sessions::session::extract_token_from_auth;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::IntoResponse;

#[allow(clippy::unwrap_used)]
pub async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> crate::error::Result<impl IntoResponse> {
    // no demo user -> no auth required
    if state.config.auth.demo_user.is_empty() || state.config.auth.demo_password.is_empty() {
        return Ok(next.run(req).await);
    }

    let Some(token) = extract_token_from_auth(req.headers()) else {
        return crate::error::MissingAuthTokenSnafu.fail()?;
    };

    let sessions = state.execution_svc.get_sessions().await; // `get_sessions` returns an RwLock

    let sessions = sessions.read().await;

    if !sessions.contains_key(&token) {
        return crate::error::InvalidAuthTokenSnafu.fail()?;
    }
    //Dropping the lock guard before going to the next request
    drop(sessions);

    Ok(next.run(req).await)
}
