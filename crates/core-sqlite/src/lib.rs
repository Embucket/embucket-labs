mod lock_manager;
mod handle;
mod vfs;
mod sqlite_config;
mod error;

use tokio::runtime::Handle;
use slatedb::Db;
use std::sync::{Arc};
use error::{self as sqlite_error, Result};
use snafu::{ResultExt, OptionExt};
use std::sync::Mutex;
use std::collections::HashMap;
use std::sync::OnceLock;
use r2d2_sqlite::SqliteConnectionManager;
use r2d2::{PooledConnection, Pool};

const DEFAULT_DB_NAME: &str = "embucket.db";

unsafe extern "C" {
    fn initialize_grpsqlite() -> i32;
}

static SQLITE_STORE: OnceLock<Arc<SqliteStore>> = OnceLock::new();

// Sqlite Store is singleton.
pub struct SqliteStore {
    connections: Mutex<HashMap<String, r2d2::Pool<SqliteConnectionManager>>>,
}

impl SqliteStore {
    pub fn conn(&self, db_name: &str) -> Result<PooledConnection<SqliteConnectionManager>> {
        if let Ok(mut connections) = self.connections.lock() {
            if let Some(conn) = connections.get(db_name) {
                Ok(conn.get().context(sqlite_error::R2d2Snafu)?)
            } else {
                let manager = SqliteConnectionManager::file(db_name);
                let pool = Pool::new(manager)
                    .context(sqlite_error::R2d2Snafu)?;
                let conn = pool.get()
                    .context(sqlite_error::R2d2Snafu)?;
                connections.insert(db_name.to_string(), pool);
                Ok(conn)
            }
        } else {
            sqlite_error::ConnectionsLockSnafu.fail()?
        }
    }

    pub fn default_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>> {
        self.conn(DEFAULT_DB_NAME)
    }

    pub fn current() -> Result<Arc<Self>> {
        Ok(SQLITE_STORE.get().context(sqlite_error::SqliteNotInitializedYetSnafu)?.clone())
    }

    #[tracing::instrument(name = "SqliteStore::init", skip(db), err)]
    pub fn init(db: Arc<Db>) -> Result<Arc<Self>> {
        if let Ok(current) = Self::current() {
            return Ok(current);
        }
        let runtime = Handle::current();
        vfs::set_vfs_context(runtime, db);

        // Initialize grpsqlite VFS
        tracing::info!("Initializing grpsqlite VFS...");
        unsafe { initialize_grpsqlite() };

        let sqlite_store = Self {
            connections: Mutex::new(HashMap::new()),
        };

        // Open database connection
        let connection = sqlite_store.default_conn()?;

        // Test VFS with pragma
        let mut vfs_detected = false;
        if let Ok(mut stmt) = connection.prepare("PRAGMA is_memory_server") {
            let mut rows = stmt.query([])
                .context(sqlite_error::RusqliteSnafu)?;
            if let Ok(Some(row)) = rows.next() {
                if let Ok(result) = row.get::<usize, String>(0) {
                    log::debug!("result: {result}");
                    vfs_detected = result == "maybe?";
                }
            }
        }

        if !vfs_detected {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        let sqlite_store = Arc::new(sqlite_store);
        if SQLITE_STORE.set(sqlite_store.clone()).is_err() {
            return sqlite_error::FailedToInitializeSqliteStoreSnafu.fail();
        }
        sqlite_store.self_check()?;
        Ok(sqlite_store)
    }

    fn self_check(&self) -> Result<()> {
        self
            .default_conn()?
            .execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])
            .context(sqlite_error::RusqliteSnafu)?;

        let mut check_passed = false;
        if let Ok(mut stmt) = self.default_conn()?
            .prepare("SELECT name FROM sqlite_schema WHERE type ='table'")
        {
            let mut rows = stmt.query([])
                .context(sqlite_error::RusqliteSnafu)?;
            if let Ok(Some(row)) = rows.next() {
                if let Ok(result) = row.get::<usize, String>(0) {
                    tracing::info!("result: {result}");
                    check_passed = true;
                }
            }
        }
        assert!(check_passed, "Sqlite VFS didn't pass runtime self check");
        Ok(())
    }
}