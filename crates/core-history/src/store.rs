use core_utils::Db;
use std::sync::Arc;

pub struct SlateDBWorksheetsStore {
    pub db: Db,
}

impl std::fmt::Debug for SlateDBWorksheetsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBWorksheetsStore").finish()
    }
}

impl SlateDBWorksheetsStore {
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    pub async fn new_in_memory() -> Arc<Self> {
        Arc::new(Self::new(Db::memory().await))
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }
}
