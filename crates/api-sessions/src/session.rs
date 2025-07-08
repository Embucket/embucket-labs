use axum::{Json, extract::FromRequestParts, response::IntoResponse};
use core_executor::service::ExecutionService;
use core_executor::session::SESSION_INACTIVITY_EXPIRATION_SECONDS;
use http::header::COOKIE;
use http::request::Parts;
use http::{HeaderMap, HeaderName};
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use time::{Duration, OffsetDateTime};
use tower_sessions::{
    ExpiredDeletion, Expiry, Session, SessionStore,
    session::{Id, Record},
    session_store,
};
use uuid;

pub const SESSION_ID_COOKIE_NAME: &str = "session_id";

pub const SESSION_EXPIRATION_SECONDS: u64 = 60;

#[derive(Clone)]
pub struct RequestSessionStore {
    execution_svc: Arc<dyn ExecutionService>,
}

#[allow(clippy::missing_const_for_fn)]
impl RequestSessionStore {
    pub fn new(execution_svc: Arc<dyn ExecutionService>) -> Self {
        Self { execution_svc }
    }

    pub async fn continuously_delete_expired(
        self,
        period: tokio::time::Duration,
    ) -> session_store::Result<()> {
        let mut interval = tokio::time::interval(period);
        interval.tick().await; // The first tick completes immediately; skip.
        loop {
            interval.tick().await;
            self.delete_expired().await?;
        }
    }
}

#[async_trait::async_trait]
impl SessionStore for RequestSessionStore {
    #[tracing::instrument(name = "SessionStore::create", level = "trace", skip(self), err, ret)]
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
            let sessions = self.execution_svc.get_sessions().await;

            let mut sessions = sessions.write().await;

            if let Some(session) = sessions.get_mut(df_session_id) {
                let mut expiry = session.expiry.lock().await;
                *expiry = record.expiry_date;
                tracing::debug!("Updating expiry: {}", *expiry);
            } else {
                drop(sessions);
                self.execution_svc
                    .create_session(df_session_id.to_string())
                    .await
                    .map_err(|e| session_store::Error::Backend(e.to_string()))?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(name = "SessionStore::save", level = "trace", skip(self), err, ret)]
    async fn save(&self, _record: &Record) -> session_store::Result<()> {
        Ok(())
    }

    #[tracing::instrument(name = "SessionStore::load", level = "trace", skip(self), err, ret)]
    async fn load(&self, _id: &Id) -> session_store::Result<Option<Record>> {
        Ok(None)
    }

    #[tracing::instrument(name = "SessionStore::delete", level = "trace", skip(self), err, ret)]
    async fn delete(&self, _id: &Id) -> session_store::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExpiredDeletion for RequestSessionStore {
    #[tracing::instrument(
        name = "ExpiredDeletion::delete_expired",
        level = "trace",
        skip(self),
        err,
        ret
    )]
    async fn delete_expired(&self) -> session_store::Result<()> {
        let sessions = self.execution_svc.get_sessions().await;

        let mut sessions = sessions.write().await;

        let now = OffsetDateTime::now_utc();
        tracing::debug!("Starting to delete expired for: {}", now);
        //Sadly can't use `sessions.retain(|_, session| { ... }`, since the `OffsetDatetime` is in a `Mutex`
        let mut session_ids = Vec::new();
        for (session_id, session) in sessions.iter() {
            let expiry = session.expiry.lock().await;
            if *expiry <= now {
                session_ids.push(session_id.clone());
            }
        }

        for session_id in session_ids {
            tracing::debug!("Deleting expired: {}", session_id);
            sessions.remove(&session_id);
        }

        Ok(())
    }
}

impl std::fmt::Debug for RequestSessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestSessionStore")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct DFSessionId(pub String);

impl<S> FromRequestParts<S> for DFSessionId
where
    S: Send + Sync,
{
    type Rejection = SessionError;

    async fn from_request_parts(req: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let session = Session::from_request_parts(req, state).await.map_err(|e| {
            tracing::error!("Failed to get session: {}", e.1);
            SessionError::SessionLoad {
                msg: e.1.to_string(),
            }
        })?;
        //If UI Auth middleware generated a new session id
        let session_id = if let Some(Self(session_id)) = req.extensions.get::<Self>() {
            tracing::debug!(
                "Found DF session_id in extensions for creation: {}",
                session_id
            );
            Self::create_session(&session, session_id.clone()).await
        //If the session is alive
        } else if let Some(token) = extract_token(&req.headers) {
            tracing::debug!("Found DF session_id in headers: {}", token);
            session
                .insert("DF_SESSION_ID", token.clone())
                .await
                .context(SessionPersistSnafu)?;
            session.set_expiry(Some(Expiry::OnInactivity(Duration::seconds(
                SESSION_INACTIVITY_EXPIRATION_SECONDS,
            ))));
            session.save().await.context(SessionPersistSnafu)?;
            Ok(Self(token))
        //If the session is dead
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            Self::create_session(&session, id.clone()).await
        };
        //We don't rely on their cookie :)
        session.flush().await.context(SessionPersistSnafu)?;
        session_id
    }
}

impl DFSessionId {
    async fn create_session(session: &Session, id: String) -> Result<Self, SessionError> {
        tracing::debug!("Creating new DF session_id: {}", id);
        session
            .insert("DF_SESSION_ID", id.clone())
            .await
            .context(SessionPersistSnafu)?;
        session.save().await.context(SessionPersistSnafu)?;
        Ok(Self(id))
    }
}

#[must_use]
pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    //First we check the header, second the cookie
    headers
        .get("authorization")
        .and_then(|value| {
            value.to_str().ok().and_then(|auth| {
                #[allow(clippy::unwrap_used)]
                let re = Regex::new(r#"Snowflake Token="([a-f0-9\-]+)""#).unwrap();
                re.captures(auth)
                    .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
            })
        })
        .map_or_else(
            || {
                let cookies = cookies_from_header(headers, COOKIE);
                cookies
                    .get(SESSION_ID_COOKIE_NAME)
                    .map(|str_ref| (*str_ref).to_string())
            },
            Some,
        )
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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SessionError {
    #[snafu(display("Session load error: {msg}"))]
    SessionLoad { msg: String },
    #[snafu(display("Unable to persist session {source:?}"))]
    SessionPersist {
        source: tower_sessions::session::Error,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoResponse for SessionError {
    fn into_response(self) -> axum::response::Response {
        let er = ErrorResponse {
            message: self.to_string(),
            status_code: 500,
        };
        (http::StatusCode::INTERNAL_SERVER_ERROR, Json(er)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_executor::models::QueryContext;
    use core_executor::service::ExecutionService;
    use core_executor::service::make_text_execution_svc;
    use serde_json::json;
    use std::collections::HashMap;
    use time::OffsetDateTime;
    use tokio::time::sleep;
    use tower_sessions::SessionStore;
    use tower_sessions::session::{Id, Record};

    #[tokio::test]
    #[allow(clippy::expect_used, clippy::too_many_lines)]
    async fn test_expiration() {
        let execution_svc = make_text_execution_svc().await;

        let df_session_id = "fasfsafsfasafsass".to_string();
        execution_svc
            .create_session(df_session_id.clone())
            .await
            .expect("Failed to create a session");

        let session_store = RequestSessionStore::new(execution_svc.clone());

        let data = HashMap::new();
        let mut record = Record {
            id: Id::default(),
            data,
            expiry_date: OffsetDateTime::now_utc(),
        };
        record
            .data
            .insert("DF_SESSION_ID".to_string(), json!(df_session_id.clone()));
        tokio::task::spawn(
            session_store
                .clone()
                .continuously_delete_expired(tokio::time::Duration::from_secs(5)),
        );
        let () = session_store
            .create(&mut record)
            .await
            .expect("Failed to get a session");
        let () = sleep(core::time::Duration::from_secs(11)).await;
        execution_svc
            .query(&df_session_id, "SELECT 1", QueryContext::default())
            .await
            .expect_err("Failed to execute query (session deleted)");
    }
}
