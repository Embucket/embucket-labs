use crate::Result;
use core_utils::Db;
use std::sync::Arc;

pub struct SlateDBHistoryStore {
    pub db: Db,
}

impl std::fmt::Debug for SlateDBHistoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBWorksheetsStore").finish()
    }
}

impl SlateDBHistoryStore {
    #[allow(clippy::expect_used)]
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    #[allow(clippy::expect_used)]
    pub async fn new_in_memory() -> Arc<Self> {
        // create utils db regardless of feature, but use it only with utilsdb feature
        // to avoid changing the code
        let utils_db = Db::memory().await;
        let store = Arc::new(Self::new(utils_db));
        let _ = store.init().await;
        store
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }

    pub async fn init(&self) -> Result<()> {
        cfg_if::cfg_if! {
            if #[cfg(feature = "sqlite")]
            {
                self.create_tables().await?;
            }
        }
        Ok(())
    }
}
