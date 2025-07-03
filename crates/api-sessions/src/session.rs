use axum::{Json, extract::FromRequestParts, response::IntoResponse};
use core_executor::service::ExecutionService;
use http::HeaderMap;
use http::request::Parts;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tower_sessions::{
    ExpiredDeletion, Session, SessionStore,
    session::{Id, Record},
    session_store,
};
use uuid;

pub const SESSION_ID_COOKIE_NAME: &str = "session_id";

pub type RequestSessionMemory = Arc<Mutex<HashMap<Id, Record>>>;

#[derive(Clone)]
pub struct RequestSessionStore {
    //store: Arc<Mutex<HashMap<Id, Record>>>,
    execution_svc: Arc<dyn ExecutionService>,
}

#[allow(clippy::missing_const_for_fn)]
impl RequestSessionStore {
    pub fn new(
        store: Arc<Mutex<HashMap<Id, Record>>>,
        execution_svc: Arc<dyn ExecutionService>,
    ) -> Self {
        Self {
            //store,
            execution_svc,
        }
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
        // let mut store_guard = self.store.lock().await;
        // while store_guard.contains_key(&record.id) {
        //     // Session ID collision mitigation.
        //     record.id = Id::default();
        // }
        // store_guard.insert(record.id, record.clone());

        if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
            self.execution_svc
                .create_session(df_session_id.to_string())
                .await
                .map_err(|e| session_store::Error::Backend(e.to_string()))?;
        }
        Ok(())
    }

    #[tracing::instrument(name = "SessionStore::save", level = "trace", skip(self), err, ret)]
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        //self.store.lock().await.insert(record.id, record.clone());
        Ok(())
    }

    #[tracing::instrument(name = "SessionStore::load", level = "trace", skip(self), err, ret)]
    async fn load(&self, id: &Id) -> session_store::Result<Option<Record>> {
        // Ok(self
        //     .store
        //     .lock()
        //     .await
        //     .get(id)
        //     .filter(|Record { expiry_date, .. }| *expiry_date > OffsetDateTime::now_utc())
        //     .cloned())
        Ok(None)
    }

    #[tracing::instrument(name = "SessionStore::delete", level = "trace", skip(self), err, ret)]
    async fn delete(&self, id: &Id) -> session_store::Result<()> {
        // let mut store_guard = self.store.lock().await;
        // if let Some(record) = store_guard.get(id) {
        //     if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
        //         self.execution_svc
        //             .delete_session(df_session_id.to_string())
        //             .await
        //             .map_err(|e| session_store::Error::Backend(e.to_string()))?;
        //     }
        // }
        // store_guard.remove(id);
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
        // let mut store_guard = self.store.lock().await;
        // let now = OffsetDateTime::now_utc();
        // tracing::error!("Deleting expired acquired a lock, to expire <=: {now}");
        // let expired = store_guard
        //     .iter()
        //     .filter_map(
        //         |(id, Record { expiry_date, .. })| {
        //             if *expiry_date <= now { Some(*id) } else { None }
        //         },
        //     )
        //     .collect::<Vec<_>>();
        //If it is dropped here, the session maybe updated and still get deleted
        //drop(store_guard);
        //Somewhere here we update the session expiry time, but the `delete fn` doesn't check this (and it shouldn't)
        // for id in expired {
        //     if let Some(record) = store_guard.get(&id) {
        //         if let Some(df_session_id) =
        //             record.data.get("DF_SESSION_ID").and_then(|v| v.as_str())
        //         {
        //             tracing::error!(
        //                 "Deleting expired: {df_session_id} with expiry: {}",
        //                 record.expiry_date
        //             );
        //             self.execution_svc
        //                 .delete_session(df_session_id.to_string())
        //                 .await
        //                 .map_err(|e| session_store::Error::Backend(e.to_string()))?;
        //         }
        //     }
        //     store_guard.remove(&id);
        // }
        //If here we hang since deleting also needs to acquire the lock
        //drop(store_guard);
        Ok(())
    }
}

impl std::fmt::Debug for RequestSessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestSessionStore")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
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
        // let session_id = if let Ok(Some(id)) = session.get::<String>("DF_SESSION_ID").await {
        //     tracing::error!("Found DF session_id: {}", id);
        //     session
        //         .insert("DF_SESSION_ID", id.clone())
        //         .await
        //         .context(SessionPersistSnafu)?;
        //     session.save().await.context(SessionPersistSnafu)?;
        //     tracing::error!("expiry date: {}", session.expiry_date());
        //     id
        // } else 
        let session_id = if let Some(token) = extract_token(&req.headers) {
            tracing::error!("Found DF session_id in headers: {}", token);
            // session
            //     .insert("DF_SESSION_ID", token.clone())
            //     .await
            //     .context(SessionPersistSnafu)?;
            //session.save().await.context(SessionPersistSnafu)?;
            //tracing::error!("expiry date: {}", session.expiry_date());
            token
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            tracing::error!("Creating new DF session_id: {}", id);
            session
                .insert("DF_SESSION_ID", id.clone())
                .await
                .context(SessionPersistSnafu)?;
            session.save().await.context(SessionPersistSnafu)?;
            //tracing::error!("expiry date: {}", session.expiry_date());
            id
        };
        Ok(Self(session_id))
    }
}

#[must_use]
pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    headers.get("authorization").and_then(|value| {
        value.to_str().ok().and_then(|auth| {
            #[allow(clippy::unwrap_used)]
            let re = Regex::new(r#"Snowflake Token="([a-f0-9\-]+)""#).unwrap();
            re.captures(auth)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        })
    }).map_or_else(|| {
        let cookies = cookies_from_header(headers);
        cookies.get(SESSION_ID_COOKIE_NAME).map(|str_ref| str_ref.to_string())
    }, |token| { 
        Some(token) 
    }) 
}
#[allow(clippy::explicit_iter_loop)]
pub fn cookies_from_header(headers: &HeaderMap) -> HashMap<&str, &str> {
    let mut cookies_map = HashMap::new();

    let cookies = headers.get_all(http::header::COOKIE);

    for value in cookies.iter() {
        if let Ok(cookie_str) = value.to_str() {
            for cookie in cookie_str.split(';') {
                let parts: Vec<&str> = cookie.trim().split('=').collect();
                cookies_map.insert(parts[0], parts[1]);
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

        let session_memory = RequestSessionMemory::default();
        let session_store = RequestSessionStore::new(session_memory, execution_svc.clone());

        let df_session_id = "fasfsafsfasafsass".to_string();
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
            .expect("Failed to create session");
        let () = sleep(core::time::Duration::from_secs(11)).await;
        execution_svc
            .query(&df_session_id, "SELECT 1", QueryContext::default())
            .await
            .expect_err("Failed to execute query (session deleted)");
    }
}
