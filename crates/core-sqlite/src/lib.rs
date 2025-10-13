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
use parking_lot::Mutex;
use vfs::{logger, VFS_NAME};

// using Mutex to support tests that trying to initialize all at the same time
static INITIALIZED: Mutex<OnceLock<bool>> = Mutex::new(OnceLock::new());

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
        // Initialize slatedbsqlite VFS per process
        let init = INITIALIZED.lock();
        if let Some(true) = &init.get() {
            // already initialized (by another thread, makes sense for tests)
            Ok(Self {
                db_name: db_name.to_string(),
                pool: Self::create_pool(db_name)?,
            })
        } else {
            tracing::info!("Initializing slatedbsqlite VFS...");
            let log_filename = Some("sqlite.log");
            vfs::set_vfs_context(db, log_filename);
    
            tracing::info!("slatedbsqlite VFS init start");
            let res = unsafe { initialize_slatedbsqlite() };
            tracing::info!("slatedbsqlite VFS init done: {res}");

            init.set(true).map_err(|_| sqlite_error::SqliteInitSnafu.build())?;
            // Note: other threads waiting while we release lock

            let sqlite_store = Self {
                db_name: db_name.to_string(),
                pool: Self::create_pool(db_name)?,
            };
            sqlite_store.pragmas_check().await?;
            Ok(sqlite_store)                
        }
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::conn", fields(conn_str), skip(self), err)]
    pub async fn conn(&self) -> Result<Object> {
        Ok(self.pool.get().await
            .context(sqlite_error::PoolSnafu)?)
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::create_pool", fields(conn_str), err)]
    fn create_pool(db_name: &str) -> Result<Pool> {   
        let pool = Config::new(db_name)
            .create_pool(Runtime::Tokio1)
            .context(sqlite_error::CreatePoolSnafu)?;
        Ok(pool)
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::pragmas_check", skip(self), err)]
    async fn pragmas_check(&self) -> Result<()> {
        log::info!(logger: logger(), "pragmas_check");
        let connection = self.conn().await?;

        // Test VFS with pragma, if our vfs is loaded
        let vfs_detected = connection.interact(|conn| -> SqlResult<String> {
            let pragma_vfs = format!("PRAGMA {VFS_NAME:?}");
            conn.query_row(&pragma_vfs, [], |row| row.get::<_, String>(0))
        }).await??;
        log::info!(logger: logger(), "vfs_detected={vfs_detected}");
        if vfs_detected != VFS_NAME.to_string_lossy() {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        // try enabling WAL
        let _journal_mode = connection.interact(|conn| -> SqlResult<String> {
            conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))
        }).await??;
        log::info!(logger: logger(), "JOURNAL_MODE={_journal_mode:?}");

        // check if test table exists
        let check_res = connection.interact(|conn| -> SqlResult<Vec<String>> {
            conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])?;
            let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type ='table'")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?
                .filter_map(SqlResult::ok)
                .collect::<Vec<_>>();
            Ok(rows)
        }).await??;

        if !check_res.contains(&"test".to_string()) {
            tracing::error!("Didn't pass check, res={check_res:?}");
            return Err(sqlite_error::SelfCheckSnafu.fail()?)
        } else {
            tracing::info!("VFS check passed");
        }

        Ok(())
    }
}
