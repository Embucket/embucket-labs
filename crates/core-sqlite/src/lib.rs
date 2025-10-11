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

        let log_filename = Some(format!("{}.log", db_name.replace(".", "_")));
        vfs::set_vfs_context(db, log_filename);

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

        let pool = Self::_create_pool(db_name).await?;

        let sqlite_store = Self {
            db_name: db_name.to_string(),
            pool,
        };

        sqlite_store.more_inits_and_self_check().await?;
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

    #[tracing::instrument(level = "debug", name = "SqliteStore::init_logger", skip(self), err)]
    async fn query(&self, sql: &str) -> Result<String> {
        let connection = self.conn().await?;
        let sql_cloned = sql.to_string();
        let res = connection.interact(move |conn| -> SqlResult<usize> {
            let res = conn.query_row(&sql_cloned, [], |row| row.get(0))?;
            Ok(res)
        })
        .await??;
        Ok(res.to_string())
    }

    #[tracing::instrument(level = "debug", name = "SqliteStore::more_inits_and_self_check", skip(self), err)]
    async fn more_inits_and_self_check(&self) -> Result<()> {
        let connection = self.conn().await?;

        // Test VFS with pragma, if our vfs is loaded
        let is_vfs = connection.interact(|conn| -> SqlResult<Option<String>> {
            let row = conn
                .prepare("PRAGMA slatedb_vfs")?
                .query([])?
                .next()?
                .map(|row| -> SqlResult<Option<String>> { row.get(0) });
            Ok(row)
        })
        .await??;

        let vfs_detected = is_vfs == "maybe?";
        if !vfs_detected {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        let _ = self.execute("PRAGMA journal_mode = WAL").await?;

        let _ = self.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)").await?;

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
