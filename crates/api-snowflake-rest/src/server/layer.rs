use super::{error, state::AppState};
use api_sessions::session::extract_token_from_auth;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::IntoResponse;

#[allow(clippy::unwrap_used)]
#[tracing::instrument(
    name = "api_snowflake_rest::layer::require_auth",
    level = "trace",
    skip(state, req, next),
    fields(request_headers = format!("{:#?}", req.headers()), response_headers, session_id),
    err,
)]
pub async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> error::Result<impl IntoResponse> {
    // no demo user -> no auth required
    if state.config.auth.demo_user.is_empty() || state.config.auth.demo_password.is_empty() {
        return Ok(next.run(req).await);
    }

    let Some(token) = extract_token_from_auth(req.headers()) else {
        return error::MissingAuthTokenSnafu.fail()?;
    };

    // Record the result as part of the current span.
    tracing::Span::current().record("session_id", token.as_str());

    let sessions = state.execution_svc.get_sessions(); // `get_sessions` returns an RwLock

    let sessions = sessions.read().await;

    if !sessions.contains_key(&token) {
        return error::InvalidAuthTokenSnafu.fail()?;
    }
    //Dropping the lock guard before going to the next request
    drop(sessions);

    let response = next.run(req).await;

    // Record the result as part of the current span.
    tracing::Span::current().record("response_headers", format!("{:#?}", response.headers()));

    Ok(response)
}
