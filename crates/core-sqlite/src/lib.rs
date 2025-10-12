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
        // Note: To use dedicated runtime for sqlite it should be passed externally from sync context

        let log_filename = Some("sqlite.log");
        vfs::set_vfs_context(db, log_filename);

        // Initialize slatedbsqlite VFS per process
        tracing::info!("Initializing slatedbsqlite VFS...");
        let init = INITIALIZED.lock();
        if let Some(true) = &init.get() {
            tracing::info!("slatedbsqlite VFS already initialized");
        } else {
            let res = unsafe { initialize_slatedbsqlite() };
            tracing::info!("slatedbsqlite VFS init: {}", res);
            init.set(true)
                .map_err(|_| sqlite_error::SqliteInitSnafu.build())?;
        }

        let pool = Self::_create_pool(db_name).await?;

        let sqlite_store = Self {
            db_name: db_name.to_string(),
            pool,
        };
        sqlite_store.pragmas_check().await?;
        Ok(sqlite_store)
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::conn", fields(conn_str), skip(self), err)]
    pub async fn conn(&self) -> Result<Object> {
        Ok(self.pool.get().await
            .context(sqlite_error::PoolSnafu)?
        )
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::create_pool", fields(conn_str), err)]
    async fn _create_pool(db_name: &str) -> Result<Pool> {
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
        Ok(pool)
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::pragmas_check", skip(self), err)]
    async fn pragmas_check(&self) -> Result<()> {
        let connection = self.conn().await?;

        // Test VFS with pragma, if our vfs is loaded
        let vfs_detected = connection.interact(|conn| -> SqlResult<String> {
            conn.query_one("PRAGMA slatedb_vfs", [], |row| row.get::<_, String>(0))
        }).await??;
        if vfs_detected != "maybe?" {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        // try enabling WAL
        let journal_mode = connection.interact(|conn| -> SqlResult<String> {
            conn.query_one("PRAGMA journal_mode = WAL", [], |row| row.get::<_, String>(0))
        }).await??;
        tracing::info!("JOURNAL_MODE={journal_mode}");

        // check if test table exists
        let test_sql = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)";
        let check_res = connection.interact(|conn| -> SqlResult<String> {
            conn.query_one(test_sql, [], |row| row.get::<_, String>(0))
        }).await??;
        if check_res != "test" {
            tracing::info!("Didn't pass check, res={check_res}");
            return Err(sqlite_error::SelfCheckSnafu.fail()?)
        }
        Ok(())
    }
}
