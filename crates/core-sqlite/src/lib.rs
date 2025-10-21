pub mod error;

#[cfg(feature = "vfs")]
pub mod vfs;

pub use error::*;

use cfg_if::cfg_if;
use deadpool_sqlite::{Config, Object, Pool, Runtime};
use error::{self as sqlite_error};
use rusqlite::Result as SqlResult;
use slatedb::Db;
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Clone)]
pub struct SqliteDb {
    #[allow(dead_code)]
    pub db_name: String,
    pool: Pool,
}

#[tracing::instrument(level = "debug", name = "SqliteDb::create_pool", fields(conn_str), err)]
fn create_pool(db_name: &str) -> Result<Pool> {
    let pool = Config::new(db_name)
        .create_pool(Runtime::Tokio1)
        .context(sqlite_error::CreatePoolSnafu)?;
    Ok(pool)
}

impl SqliteDb {
    #[tracing::instrument(name = "SqliteDb::new", skip(_db), err)]
    #[allow(clippy::expect_used)]
    pub async fn new(_db: Arc<Db>, db_name: &str) -> Result<Self> {
        cfg_if! {
            if #[cfg(feature = "vfs")] {
                vfs::init(_db);

                // Actually pool can be used per process, and cargo test runs tests in parallel in separate threads
                // but it is overkill trying to re-use it across all the test threads
                // So here we create pool just per SqliteDb instance

                let sqlite_store = Self {
                    db_name: db_name.to_string(),
                    pool: create_pool(db_name)?,
                };

                let connection = sqlite_store.conn().await?;
                vfs::pragma_setup(&connection).await?;

                return Ok(sqlite_store);
            } else {
                let sqlite_store = Self {
                    db_name: db_name.to_string(),
                    pool: create_pool(db_name)?,
                };
                let connection = sqlite_store.conn().await?;
                // enable WAL
                connection.interact(|conn| -> SqlResult<()> {
                    let journal_mode = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))?;
                    tracing::debug!("journal_mode={journal_mode}");
                    let busy_timeout = conn.query_row("PRAGMA busy_timeout = 2000", [], |row| row.get::<_, i32>(0))?;
                    tracing::debug!("busy_timeout={busy_timeout}");
                    Ok(())
                }).await??;
                return Ok(sqlite_store);
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "SqliteDb::conn",
        fields(conn_str),
        skip(self),
        err
    )]
    pub async fn conn(&self) -> Result<Object> {
        self.pool.get().await.context(sqlite_error::PoolSnafu)
    }
}
