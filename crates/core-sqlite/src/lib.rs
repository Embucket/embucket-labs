mod lock_manager;
mod handle;
mod vfs;
mod sqlite_config;
pub mod error;

use tokio::runtime::Handle;
use slatedb::Db;
use std::sync::{Arc};
use error::{self as sqlite_error, Result};
use snafu::{ResultExt, OptionExt};
use std::sync::OnceLock;
use dashmap::DashMap;
use rusqlite::Result as SqlResult;
use deadpool_sqlite::{Config, Manager, Object, Runtime, Pool};

const DEFAULT_DB_NAME: &str = "embucket.db";

unsafe extern "C" {
    fn initialize_grpsqlite() -> i32;
}

static SQLITE_STORE: OnceLock<Arc<SqliteStore>> = OnceLock::new();

// Sqlite Store is singleton.
pub struct SqliteStore {
    connections: DashMap<String, Pool>,
}

impl SqliteStore {
    #[tracing::instrument(level = "debug", name = "SqliteStore::conn", fields(conn_str), skip(self), err)]
    pub async fn conn(&self, db_name: &str) -> Result<Object> {

        let cfg = Config::new(db_name);
        let pool = cfg.create_pool(Runtime::Tokio1)
            .context(sqlite_error::CreatePoolSnafu)?;
    
        let conn = pool.get().await.context(sqlite_error::PoolSnafu)?;
        Ok(conn)
    }

    pub async fn default_conn(&self) -> Result<Object> {
        self.conn(DEFAULT_DB_NAME).await
    }

    pub fn current() -> Result<Arc<Self>> {
        Ok(SQLITE_STORE.get().context(sqlite_error::SqliteNotInitializedYetSnafu)?.clone())
    }

    #[tracing::instrument(name = "SqliteStore::init", skip(db), err)]
    pub async fn init(db: Arc<Db>) -> Result<Arc<Self>> {
        if let Ok(current) = Self::current() {
            return Ok(current);
        }
        let runtime = Handle::current();
        vfs::set_vfs_context(runtime, db);

        // Initialize grpsqlite VFS
        tracing::info!("Initializing grpsqlite VFS...");
        unsafe { initialize_grpsqlite() };

        let sqlite_store = Self {
            connections: DashMap::new(),
        };

        // Open database connection
        let connection = sqlite_store.default_conn().await?;

        // Test VFS with pragma
        let is_vfs = connection.interact(|conn| -> SqlResult<String> {
            // TODO: Check how to verify VFS, instead of "is memory server"
            let mut stmt = conn.prepare("PRAGMA is_memory_server")?;
            let mut rows = stmt.query([])?;
            let row = rows.next()?.unwrap();
            row.get(0)
        })
        .await
        .context(sqlite_error::InteractSnafu)?
        .context(sqlite_error::RusqliteSnafu)?;

        let vfs_detected = is_vfs == "maybe?";
        if !vfs_detected {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        let sqlite_store = Arc::new(sqlite_store);
        if SQLITE_STORE.set(sqlite_store.clone()).is_err() {
            return sqlite_error::FailedToInitializeSqliteStoreSnafu.fail();
        }
        sqlite_store.self_check().await?;
        Ok(sqlite_store)
    }

    async fn self_check(&self) -> Result<()> {
        let _ = self
            .default_conn().await?
            .interact(|conn| {
                conn
                .execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])
            })
            .await
            .context(sqlite_error::InteractSnafu)?
            .context(sqlite_error::RusqliteSnafu)?;

        let mut check_passed = false;
        let connection = self.default_conn().await?;
        let result = connection.interact(|conn| -> SqlResult<Vec<String>> {
            let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type ='table'")?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(row.get(0)?);
            }
            Ok(out)
        })
        .await
        .context(sqlite_error::InteractSnafu)?
        .context(sqlite_error::RusqliteSnafu)?;
       
        tracing::info!("result: {result:?}");
        check_passed = result == ["test"];

        assert!(check_passed, "Sqlite VFS didn't pass runtime self check");
        Ok(())
    }
}