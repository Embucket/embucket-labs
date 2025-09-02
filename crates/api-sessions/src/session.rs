use crate::error as session_error;
use axum::extract::FromRequestParts;
use core_executor::ExecutionAppState;
use core_executor::service::ExecutionService;
use http::header::COOKIE;
use http::request::Parts;
use http::{HeaderMap, HeaderName};
use regex::Regex;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc};

pub const SESSION_ID_COOKIE_NAME: &str = "session_id";

pub const SESSION_EXPIRATION_SECONDS: u64 = 60;

#[derive(Clone)]
pub struct SessionStore {
    pub execution_svc: Arc<dyn ExecutionService>,
}

impl SessionStore {
    pub fn new(execution_svc: Arc<dyn ExecutionService>) -> Self {
        Self { execution_svc }
    }
    pub async fn continuously_delete_expired(&self, period: tokio::time::Duration) {
        let mut interval = tokio::time::interval(period);
        interval.tick().await; // The first tick completes immediately; skip.
        loop {
            interval.tick().await;
            let _ = self.execution_svc.delete_expired_sessions().await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFSessionId(pub String);

impl<S> FromRequestParts<S> for DFSessionId
where
    S: Send + Sync + ExecutionAppState,
{
    type Rejection = session_error::Error;

    #[allow(clippy::unwrap_used)]
    #[tracing::instrument(level = "debug", skip(req, state), fields(session_id, located_at))]
    async fn from_request_parts(req: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let execution_svc = state.get_execution_svc();

        let (session_id, located_at) = if let Some(token) = extract_token_from_auth(&req.headers) {
            (token, "auth header")
        } else {
            //This is guaranteed by the `propagate_session_cookie`, so we can unwrap
            let Self(token) = req.extensions.get::<Self>().unwrap();
            (token.clone(), "extensions")
        };

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("located_at", located_at)
            .record("session_id", session_id.clone());

        Self::get_or_create_session(execution_svc, session_id).await
    }
}

impl DFSessionId {
    #[tracing::instrument(level = "info", skip(execution_svc), fields(sessions_count))]
    async fn get_or_create_session(
        execution_svc: Arc<dyn ExecutionService>,
        session_id: String,
    ) -> Result<Self, session_error::Error> {
        if !execution_svc
            .update_session_expiry(&session_id)
            .await
            .context(session_error::ExecutionSnafu)?
        {
            let _ = execution_svc
                .create_session(&session_id)
                .await
                .context(session_error::ExecutionSnafu)?;
        }

        let sessions_count = execution_svc.get_sessions().read().await.len();
        // Record the result as part of the current span.
        tracing::Span::current().record("sessions_count", sessions_count);        

        Ok(Self(session_id))
    }
}

//Snowflake token extraction lives in the api-session crate (used here also),
// so to not create a cyclic dependency exporting it from the api-snowflake-rest crate.
// Where it's used in the `require_auth` layer as part of the session flow and where it was originally from.
#[must_use]
pub fn extract_token_from_auth(headers: &HeaderMap) -> Option<String> {
    //First we check the header
    headers.get("authorization").and_then(|value| {
        value.to_str().ok().and_then(|auth| {
            #[allow(clippy::unwrap_used)]
            let re = Regex::new(r#"Snowflake Token="([a-f0-9\-]+)""#).unwrap();
            re.captures(auth)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        })
    })
}

#[must_use]
pub fn extract_token_from_cookie(headers: &HeaderMap) -> Option<String> {
    let cookies = cookies_from_header(headers, COOKIE);
    cookies
        .get(SESSION_ID_COOKIE_NAME)
        .map(|str_ref| (*str_ref).to_string())
}

#[allow(clippy::explicit_iter_loop)]
pub fn cookies_from_header(headers: &HeaderMap, header_name: HeaderName) -> HashMap<&str, &str> {
    let mut cookies_map = HashMap::new();

    let cookies = headers.get_all(header_name);

    for value in cookies.iter() {
        if let Ok(cookie_str) = value.to_str() {
            for cookie in cookie_str.split(';') {
                let parts: Vec<&str> = cookie.trim().split('=').collect();
                if parts.len() > 1 {
                    cookies_map.insert(parts[0], parts[1]);
                }
            }
        }
    }
    cookies_map
}

#[cfg(test)]
mod tests {
    use crate::session::SessionStore;
    use core_executor::models::QueryContext;
    use core_executor::service::ExecutionService;
    use core_executor::service::make_test_execution_svc;
    use core_executor::session::to_unix;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use time::OffsetDateTime;
    use tokio::time::sleep;

    #[tokio::test]
    #[allow(clippy::expect_used, clippy::too_many_lines)]
    async fn test_expiration() {
        let execution_svc = make_test_execution_svc().await;

        let df_session_id = "fasfsafsfasafsass".to_string();
        let user_session = execution_svc
            .create_session(&df_session_id)
            .await
            .expect("Failed to create a session");

        user_session
            .expiry
            .store(to_unix(OffsetDateTime::now_utc()), Ordering::Relaxed);

        let session_store = SessionStore::new(execution_svc.clone());

        tokio::task::spawn({
            let session_store = session_store.clone();
            async move {
                session_store
                    .continuously_delete_expired(Duration::from_secs(5))
                    .await;
            }
        });

        let () = sleep(Duration::from_secs(7)).await;
        execution_svc
            .query(&df_session_id, "SELECT 1", QueryContext::default())
            .await
            .expect_err("Failed to execute query (session deleted)");
    }
}
