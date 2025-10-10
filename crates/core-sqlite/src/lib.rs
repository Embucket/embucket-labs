mod lock_manager;
mod handle;
mod vfs;
pub mod error;

pub use error::*;

use slatedb::Db;
use std::sync::{Arc, OnceLock};
use error::{self as sqlite_error};
use snafu::ResultExt;
use rusqlite::Result as SqlResult;
use deadpool_sqlite::{Config, Object, Runtime, Pool};

static INITIALIZED: OnceLock<bool> = OnceLock::new();

unsafe extern "C" {
    fn initialize_slatedbsqlite() -> i32;
}

#[derive(Clone)]
pub struct SqliteStore {
    db_name: String,
    pool: Pool,
}

impl SqliteStore {
    #[tracing::instrument(name = "SqliteStore::new", skip(db), err)]
    #[allow(clippy::expect_used)]
    pub async fn new(db: Arc<Db>, db_name: &str) -> Result<Self> {
        // Note: To use dedicated runtime for sqlite it should be passed externally from sync context

        let db_name_cloned = db_name.to_string();
        // using spawn_block is noop here
        let runtime = tokio::runtime::Handle::current();
        let pool = runtime.spawn_blocking(move || -> Result<Pool> {
            // deadpool captures handle of the runtime where pool is created
            Config::new(db_name_cloned).create_pool(Runtime::Tokio1)
                .context(sqlite_error::CreatePoolSnafu)
        })
        .await
        .context(sqlite_error::SpawnBlockingSnafu)??;

        let sqlite_store = Self {
            db_name: db_name.to_string(),
            pool,
        };
        let log = Some(format!("{}.log", sqlite_store.db_name));
        vfs::set_vfs_context(db, log);

        // Initialize slatedbsqlite VFS per process
        tracing::info!("Initializing slatedbsqlite VFS...");
        if let Some(true) = &INITIALIZED.get() {
            tracing::info!("slatedbsqlite VFS already initialized");
        } else {
            let res = unsafe { initialize_slatedbsqlite() };
            tracing::info!("slatedbsqlite VFS init: {}", res);
            INITIALIZED.set(true)
                .map_err(|_| sqlite_error::SqliteNotInitializedYetSnafu.build())?;
        }

        sqlite_store.self_check().await?;
        Ok(sqlite_store)
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::conn", fields(conn_str), skip(self), err)]
    pub async fn conn(&self) -> Result<Object> {
        Ok(self.pool.get().await
            .context(sqlite_error::PoolSnafu)?
        )
    }

    async fn self_check(&self) -> Result<()> {
        let connection = self.conn().await?;

        // Test VFS with pragma, if our vfs is loaded
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

        let _ = connection.interact(|conn| -> SqlResult<usize> {
                conn
                .execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])
            })
            .await
            .context(sqlite_error::DeadpoolSnafu)?
            .context(sqlite_error::RusqliteSnafu)?;

        // check if test table exists
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
              
        let check_passed = result == ["test"];
        if !check_passed {
            tracing::info!("result: {result:?}");
            return Err(sqlite_error::SelfCheckSnafu.fail()?)
        }
        Ok(())
    }
}
