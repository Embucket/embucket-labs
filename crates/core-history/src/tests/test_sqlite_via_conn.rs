use crate::SlateDBHistoryStore;
use core_sqlite::{self as core_sqlite_error, Result as SqliteResult};
use rusqlite::Result as SqlResult;
use snafu::ResultExt;
use tokio;

#[tokio::test]
async fn test_sqlite_history_schema() -> SqliteResult<()> {
    let history_store = SlateDBHistoryStore::new_in_memory().await;

    let res = history_store
        .db
        .sqlite_history_store
        .conn()
        .await?
        .interact(|conn| -> SqlResult<usize> {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)",
                [],
            )
        })
        .await
        .context(core_sqlite_error::DeadpoolSnafu)?
        .context(core_sqlite_error::RusqliteSnafu)?;
    assert_eq!(res, 0);
    Ok(())
}
