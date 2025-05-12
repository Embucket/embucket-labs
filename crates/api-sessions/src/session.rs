use axum::{Json, extract::FromRequestParts, response::IntoResponse};
use core_executor::service::ExecutionService;
use http::request::Parts;
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

pub type RequestSessionMemory = Arc<Mutex<HashMap<Id, Record>>>;

#[derive(Clone)]
pub struct RequestSessionStore {
    store: Arc<Mutex<HashMap<Id, Record>>>,
    execution_svc: Arc<dyn ExecutionService>,
}

#[allow(clippy::missing_const_for_fn)]
impl RequestSessionStore {
    pub fn new(
        store: Arc<Mutex<HashMap<Id, Record>>>,
        execution_svc: Arc<dyn ExecutionService>,
    ) -> Self {
        Self {
            store,
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
    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let mut store_guard = self.store.lock().await;
        while store_guard.contains_key(&record.id) {
            // Session ID collision mitigation.
            record.id = Id::default();
        }
        store_guard.insert(record.id, record.clone());

        if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
            self.execution_svc
                .create_session(df_session_id.to_string())
                .await
                .map_err(|e| session_store::Error::Backend(e.to_string()))?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.store.lock().await.insert(record.id, record.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    async fn load(&self, id: &Id) -> session_store::Result<Option<Record>> {
        Ok(self
            .store
            .lock()
            .await
            .get(id)
            .filter(|Record { expiry_date, .. }| *expiry_date > OffsetDateTime::now_utc())
            .cloned())
    }

    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    async fn delete(&self, id: &Id) -> session_store::Result<()> {
        let mut store_guard = self.store.lock().await;
        if let Some(record) = store_guard.get(id) {
            if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
                self.execution_svc
                    .delete_session(df_session_id.to_string())
                    .await
                    .map_err(|e| session_store::Error::Backend(e.to_string()))?;
            }
        }
        store_guard.remove(id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExpiredDeletion for RequestSessionStore {
    #[tracing::instrument(level = "trace", skip(self), err, ret)]
    async fn delete_expired(&self) -> session_store::Result<()> {
        let store_guard = self.store.lock().await;
        let now = OffsetDateTime::now_utc();
        let expired = store_guard
            .iter()
            .filter_map(
                |(id, Record { expiry_date, .. })| {
                    if *expiry_date <= now { Some(*id) } else { None }
                },
            )
            .collect::<Vec<_>>();
        drop(store_guard);

        for id in expired {
            self.delete(&id).await?;
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
        let session_id = if let Ok(Some(id)) = session.get::<String>("DF_SESSION_ID").await {
            tracing::debug!("Found DF session_id: {}", id);
            id
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            tracing::debug!("Creating new DF session_id: {}", id);
            session
                .insert("DF_SESSION_ID", id.clone())
                .await
                .context(SessionPersistSnafu)?;
            session.save().await.context(SessionPersistSnafu)?;
            id
        };
        Ok(Self(session_id))
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SessionError {
    #[snafu(display("Session load error: {msg}"))]
    SessionLoad { msg: String },
    #[snafu(display("Unable to persist session"))]
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
    use core_executor::query::QueryContext;
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
