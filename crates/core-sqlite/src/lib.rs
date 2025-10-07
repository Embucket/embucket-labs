mod lock_manager;
mod handle;
mod vfs;
pub mod error;

pub use error::*;

use tokio::runtime::Handle;
use slatedb::Db;
use std::sync::{Arc};
use error::{self as sqlite_error};
use snafu::{ResultExt};
use dashmap::DashMap;
use rusqlite::Result as SqlResult;
use deadpool_sqlite::{Config, Object, Runtime, Pool};

const DEFAULT_DB_NAME: &str = "embucket.db";

unsafe extern "C" {
    fn initialize_grpsqlite() -> i32;
}

// Sqlite Store is singleton.
#[derive(Clone)]
pub struct SqliteStore {
    pool: DashMap<String, Pool>,
}

impl SqliteStore {
    #[tracing::instrument(level = "debug", name = "SqliteStore::conn", fields(conn_str), skip(self), err)]
    pub async fn conn(&self, db_name: &str) -> Result<Object> {
        let pool = self.pool.entry(db_name.to_string()).or_try_insert_with(|| {
            let cfg = Config::new(db_name);
            cfg.create_pool(Runtime::Tokio1)
        }).context(sqlite_error::CreatePoolSnafu)?;
    
        let conn = pool.get().await.context(sqlite_error::PoolSnafu)?;
        Ok(conn)
    }

    pub async fn default_conn(&self) -> Result<Object> {
        self.conn(DEFAULT_DB_NAME).await
    }

    #[tracing::instrument(name = "SqliteStore::init", skip(db), err)]
    #[allow(clippy::expect_used)]
    pub async fn init(db: Arc<Db>) -> Result<Self> {
        let runtime = Handle::current();
        vfs::set_vfs_context(runtime, db);

        // Initialize grpsqlite VFS
        tracing::info!("Initializing grpsqlite VFS...");
        unsafe { initialize_grpsqlite() };

        let sqlite_store = Self {
            pool: DashMap::new(),
        };

        // Open database connection
        let connection = sqlite_store.default_conn().await?;

        // Test VFS with pragma
        let is_vfs = connection.interact(|conn| -> SqlResult<String> {
            let mut stmt = conn.prepare("PRAGMA slatedb_vfs")?;
            let mut rows = stmt.query([])?;
            if let Some(row) = rows.next()? {
                row.get(0)
            } else {
                Err(rusqlite::Error::QueryReturnedNoRows)
            }
        })
        .await
        .context(sqlite_error::DeadpoolSnafu)?
        .context(sqlite_error::RusqliteSnafu)?;

        let vfs_detected = is_vfs == "maybe?";
        if !vfs_detected {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        sqlite_store.self_check().await?;
        Ok(sqlite_store)
    }

    async fn self_check(&self) -> Result<()> {
        let _res = self
            .default_conn().await?
            .interact(|conn| -> SqlResult<usize> {
                conn
                .execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])
            })
            .await
            .context(sqlite_error::DeadpoolSnafu)?
            .context(sqlite_error::RusqliteSnafu)?;

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
        .context(sqlite_error::DeadpoolSnafu)?
        .context(sqlite_error::RusqliteSnafu)?;
       
        tracing::info!("result: {result:?}");
        
        let check_passed = result == ["test"];
        if !check_passed {
            return Err(sqlite_error::SelfCheckSnafu.fail()?)
        }
        Ok(())
    }
}