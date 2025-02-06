use time::OffsetDateTime;
use tokio::sync::Mutex;
use tower_sessions::{SessionStore, session_store, session::{Id, Record}, ExpiredDeletion};
use control_plane::service::ControlService;
use std::{collections::HashMap, sync::Arc};

pub type RequestSessionMemory = Arc<Mutex<HashMap<Id, Record>>>;

#[derive(Clone)]
pub struct RequestSessionStore {
    store: Arc<Mutex<HashMap<Id, Record>>>,
    control_svc: Arc<dyn ControlService + Send + Sync>,
}

impl RequestSessionStore {
    pub fn new(store: Arc<Mutex<HashMap<Id, Record>>>, control_svc: Arc<dyn ControlService + Send + Sync>) -> Self {
        Self {
            store,
            control_svc,
        }
    }

    pub async fn continuously_delete_expired(self, period: tokio::time::Duration) -> session_store::Result<()> {
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

    #[tracing::instrument(level="trace", skip(self), err, ret)]
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let mut store_guard = self.store.lock().await;
        while store_guard.contains_key(&record.id) {
            // Session ID collision mitigation.
            record.id = Id::default();
        }
        store_guard.insert(record.id, record.clone());

        if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
            self.control_svc.create_session(df_session_id.to_string()).await
                .map_err(|e| session_store::Error::Backend(e.to_string()))?;
        }
        Ok(())
    }

    #[tracing::instrument(level="trace", skip(self), err, ret)]
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.store.lock().await.insert(record.id, record.clone());
        Ok(())
    }

    #[tracing::instrument(level="trace", skip(self), err, ret)]
    async fn load(&self, id: &Id) -> session_store::Result<Option<Record>> {
        Ok(self
            .store
            .lock()
            .await
            .get(id)
            .filter(|Record { expiry_date, .. }| *expiry_date > OffsetDateTime::now_utc())
            .cloned())
    }

    #[tracing::instrument(level="trace", skip(self), err, ret)]
    async fn delete(&self, id: &Id) -> session_store::Result<()> {
        if let Some(record) = self.load(id).await? {
            if let Some(df_session_id) = record.data.get("DF_SESSION_ID").and_then(|v| v.as_str()) {
                self.control_svc.delete_session(df_session_id.to_string()).await
                    .map_err(|e| session_store::Error::Backend(e.to_string()))?;
            }
        }
        self.store.lock().await.remove(id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExpiredDeletion for RequestSessionStore {
    #[tracing::instrument(level="trace", skip(self), err, ret)]
    async fn delete_expired(&self) -> session_store::Result<()> {
        let store_guard = self.store.lock().await;
        let now = OffsetDateTime::now_utc();
        let expired = store_guard.iter().filter_map(|(id, Record { expiry_date, .. })| {
            if *expiry_date <= now {
                Some(*id)
            } else {
                None
            }
        }).collect::<Vec<_>>();
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