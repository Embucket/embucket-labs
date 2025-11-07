pub mod error;

pub use error::*;

use deadpool_sqlite::{Config, Object, Pool, Runtime};
use error::{self as sqlite_error};
use rusqlite::Result as SqlResult;
use snafu::ResultExt;

#[derive(Clone)]
pub struct SqliteDb {
    #[allow(dead_code)]
    pub db_name: String,
    pool: Pool,
}

#[tracing::instrument(level = "debug", name = "SqliteDb::create_pool", fields(conn_str), err)]
fn create_pool(db_name: &str) -> Result<Pool> {
    Config::new(db_name)
        .create_pool(Runtime::Tokio1)
        .context(sqlite_error::CreatePoolSnafu)
}

impl SqliteDb {
    #[tracing::instrument(name = "SqliteDb::new", err)]
    #[allow(clippy::expect_used)]
    pub async fn new(db_name: &str) -> Result<Self> {
        let sqlite_store = Self {
            db_name: db_name.to_string(),
            pool: create_pool(db_name)?,
        };
        let connection = sqlite_store.conn().await?;
        // enable WAL
        connection
            .interact(|conn| -> SqlResult<()> {
                let journal_mode =
                    conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))?;
                tracing::debug!("journal_mode={journal_mode}");
                let busy_timeout =
                    conn.query_row("PRAGMA busy_timeout = 2000", [], |row| row.get::<_, i32>(0))?;
                tracing::debug!("busy_timeout={busy_timeout}");
                Ok(())
            })
            .await??;
        return Ok(sqlite_store);
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
