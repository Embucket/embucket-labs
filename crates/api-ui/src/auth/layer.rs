use super::error::{self as auth_error, BadAuthTokenSnafu, Result};
use super::handlers::get_claims_validate_jwt_token;
use crate::state::AppState;
use api_sessions::DFSessionId;
use api_sessions::session::{SESSION_ID_COOKIE_NAME, extract_token};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::IntoResponse,
};
use http::header::SET_COOKIE;
use http::{HeaderMap, HeaderName};
use snafu::ResultExt;
use tower_sessions::cookie::{Cookie, SameSite};
use uuid;

fn get_authorization_token(headers: &HeaderMap) -> Result<&str> {
    let auth = headers.get(http::header::AUTHORIZATION);

    match auth {
        Some(auth_header) => {
            if let Ok(auth_header_str) = auth_header.to_str() {
                match auth_header_str.strip_prefix("Bearer ") {
                    Some(token) => Ok(token),
                    None => auth_error::BadAuthHeaderSnafu.fail(),
                }
            } else {
                auth_error::BadAuthHeaderSnafu.fail()
            }
        }
        None => auth_error::NoAuthHeaderSnafu.fail(),
    }
}

fn set_headers_in_flight(
    headers: &mut HeaderMap,
    header_name: HeaderName,
    name: &str,
    token: &str,
) -> Result<()> {
    headers
        .try_insert(
            header_name,
            Cookie::build((name, token))
                .http_only(true)
                .secure(true)
                .same_site(SameSite::Strict)
                .path("/")
                .to_string()
                .parse()
                .context(auth_error::ResponseHeaderSnafu)?,
        )
        .context(auth_error::SetCookieSnafu)?;

    Ok(())
}

pub async fn require_auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<impl IntoResponse> {
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

    if let Some(token) = extract_token(req.headers()) {
        let sessions = state.execution_svc.get_sessions().await; // `get_sessions` returns an RwLock

        // Step 2: Acquire the read lock on the session data
        let sessions = sessions.read().await;

        // Step 3: Check if the token is in the session data
        if !sessions.contains_key(&token) {
            drop(sessions);
            //If no session_id, get a new one to the extractor,
            // in a way that we can provide it to the response header for the browser also
            let session_id = uuid::Uuid::new_v4().to_string();
            req.extensions_mut().insert(DFSessionId(session_id.clone()));
            let mut res = next.run(req).await;
            set_headers_in_flight(
                res.headers_mut(),
                SET_COOKIE,
                SESSION_ID_COOKIE_NAME,
                session_id.as_str(),
            )?;
            return Ok(res);
        }
    }
    Ok(next.run(req).await)
}
