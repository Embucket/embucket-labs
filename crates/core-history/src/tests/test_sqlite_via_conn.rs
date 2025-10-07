use crate::SlateDBHistoryStore;
use rusqlite::Result as SqlResult;
use core_sqlite::{Result as SqliteResult, self as sqlite_error};
use snafu::ResultExt;
use tokio;

#[tokio::test]
async fn test_sqlite_history_schema() -> SqliteResult<()> {
    let history_store = SlateDBHistoryStore::new_in_memory().await;

    let res = history_store.db.sqlite.default_conn().await?.interact(|conn| -> SqlResult<usize> {
        conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)", [])
    })
    .await
    .context(sqlite_error::InteractSnafu)?
    .context(sqlite_error::RusqliteSnafu)?;
    assert_eq!(res, 0);
    Ok(())
}