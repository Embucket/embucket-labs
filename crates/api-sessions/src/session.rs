use crate::error as session_error;
use axum::extract::FromRequestParts;
use core_executor::ExecutionAppState;
use core_executor::service::ExecutionService;
use core_executor::session::SESSION_INACTIVITY_EXPIRATION_SECONDS;
use http::header::COOKIE;
use http::request::Parts;
use http::{HeaderMap, HeaderName};
use regex::Regex;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc};
use time::{Duration, OffsetDateTime};

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
            let sessions = self.execution_svc.get_sessions().await;

            let mut sessions = sessions.write().await;

            let now = OffsetDateTime::now_utc();
            tracing::trace!("Starting to delete expired for: {}", now);
            //Sadly can't use `sessions.retain(|_, session| { ... }`, since the `OffsetDatetime` is in a `Mutex`
            let mut session_ids = Vec::new();
            for (session_id, session) in sessions.iter() {
                let expiry = session.expiry.lock().await;
                if *expiry <= now {
                    session_ids.push(session_id.clone());
                }
            }

            for session_id in session_ids {
                tracing::trace!("Deleting expired: {}", session_id);
                sessions.remove(&session_id);
            }
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
    async fn from_request_parts(req: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let execution_svc = state.get_execution_svc();

        if let Some(token) = extract_token_from_auth(&req.headers) {
            tracing::debug!("Found DF session_id in auth header: {}", token);

            let id = Self::get_or_create_session(execution_svc, token.clone()).await?;

            Ok(id)
        } else {
            //This is guaranteed by the `propagate_session_cookie`, so we can unwrap
            let Self(token) = req.extensions.get::<Self>().unwrap();
            tracing::debug!("Found DF session_id in extensions: {}", token);

            let id = Self::get_or_create_session(execution_svc, token.clone()).await?;

            Ok(id)
        }
    }
}

impl DFSessionId {
    async fn get_or_create_session(
        execution_svc: Arc<dyn ExecutionService>,
        id: String,
    ) -> Result<Self, session_error::Error> {
        let sessions = execution_svc.get_sessions().await;

        let mut sessions = sessions.write().await;

        if let Some(session) = sessions.get_mut(&id) {
            let mut expiry = session.expiry.lock().await;
            *expiry = OffsetDateTime::now_utc()
                + Duration::seconds(SESSION_INACTIVITY_EXPIRATION_SECONDS);
            tracing::debug!("Updating expiry: {}", *expiry);
        } else {
            drop(sessions);
            let _ = execution_svc
                .create_session(id.clone())
                .await
                .context(session_error::ExecutionSnafu)?;
        }
        Ok(Self(id))
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
    use std::time::Duration;
    use time::OffsetDateTime;
    use tokio::time::sleep;

    #[tokio::test]
    #[allow(clippy::expect_used, clippy::too_many_lines)]
    async fn test_expiration() {
        let execution_svc = make_test_execution_svc().await;

        let df_session_id = "fasfsafsfasafsass".to_string();
        let user_session = execution_svc
            .create_session(df_session_id.clone())
            .await
            .expect("Failed to create a session");

        let mut expiry = user_session.expiry.lock().await;
        *expiry = OffsetDateTime::now_utc();
        drop(expiry);

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
