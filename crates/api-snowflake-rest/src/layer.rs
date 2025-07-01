use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::IntoResponse;
use crate::error::{self as api_snowflake_rest_error, Error, Result};
use crate::handlers::extract_token;
use crate::state::AppState;

#[allow(clippy::unwrap_used)]
pub async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<impl IntoResponse> {
    // no demo user -> no auth required
    if (state.config.auth.demo_user.is_empty()
        || state.config.auth.demo_password.is_empty()) 
        && state.config.auth.token.is_none()
    {
        return Ok(next.run(req).await);
    }

    let Some(token) = extract_token(&req.headers()) else {
        return api_snowflake_rest_error::MissingAuthTokenSnafu.fail();
    };
    
    //Safe to `.unwarp()`, since we checked before that `.is_none()` is false
    if token != state.config.auth.token.unwrap() {
        return api_snowflake_rest_error::InvalidAuthTokenSnafu.fail();
    }

    Ok(next.run(req).await)
}