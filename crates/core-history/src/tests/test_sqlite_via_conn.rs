#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]

use crate::SlateDBHistoryStore;
use core_sqlite::SqliteStore;
use core_sqlite::error::{self as sqlite_error, Result as SqliteResult};
use rusqlite::Result as SqlResult;
use snafu::{ResultExt, OptionExt};
use tokio;

#[tokio::test(flavor = "current_thread")]
async fn test_sqlite_history_schema() -> SqliteResult<()> {
    let db = SlateDBHistoryStore::new_in_memory().await;
    let sqlite_store = SqliteStore::current()?;

    let _ = sqlite_store.default_conn().await?.interact(|conn| -> SqlResult<()> {
        conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])?;
        Ok(())
    }).await;
        
    Ok(())
}