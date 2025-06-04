use super::error::{AuthError, AuthResult, BadAuthTokenSnafu};
use super::handlers::get_claims_validate_jwt_token;
use crate::state::AppState;
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

    // profiling
    let guard = pprof2::ProfilerGuardBuilder::default().frequency(1000).blocklist(&["libc", "libgcc", "pthread", "vdso"]).build().unwrap();
    
    let res = Ok(next.run(req).await);

    use std::io::Write;
    use pprof2::protos::Message;
    let ts = chrono::Utc::now().timestamp();
    match guard.report().build() {
        Ok(report) => {
            let fname = format!("prof/profile_{}.pb", ts);
            let mut file = std::fs::File::create(fname).unwrap();
            let profile = report.pprof().unwrap();
    
            let mut content = Vec::new();
            profile.encode(&mut content).unwrap();
            file.write_all(&content).unwrap();
    
            // println!("report created: {:?}", &report);
        }
        Err(_) => {}
    };

    if let Ok(report) = guard.report().build() {
        let fname = format!("prof/flamegraph_{}.svg", ts);
        let file = std::fs::File::create(fname).unwrap();
        let mut options = pprof2::flamegraph::Options::default();
        // options.image_width = Some(5000);
        report.flamegraph_with_options(file, &mut options).unwrap();
    };
    
    drop(guard);

    res
}
