use core_utils::Db;
use std::sync::Arc;
use core_sqlite::SqliteStore;

pub enum HistoryEngine {
    Sqlite, // indirect use of SlateDb via Sqlite
    UtilsDb, // direct use of SlateDb
}

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
    pub fn new(db: Db) -> Self {
        if cfg!(feature = "sqlite") {
            let _ = SqliteStore::init(db.slate_db())
                .expect("Failed to initialize sqlite store");
        }        
        Self { db }
    }


    // Create a new store with a new in-memory database
    #[allow(clippy::expect_used)]
    pub async fn new_in_memory() -> Arc<Self> {
        // create utils db regardless of feature, but use it only with utilsdb feature
        // to avoid changing the code 
        let utils_db = Db::memory().await;
        
        if cfg!(feature = "sqlite") {
            let _ = SqliteStore::init(utils_db.slate_db())
                .expect("Failed to initialize sqlite store");
        }
        Arc::new(Self::new(utils_db))
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }
}
