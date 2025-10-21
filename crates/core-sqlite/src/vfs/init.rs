use crate::error::*;

use super::{VFS_NAME, logger, set_vfs_context};
use deadpool_sqlite::Object;
use parking_lot::Mutex;
use rusqlite::Result as SqlResult;
use slatedb::Db;
use std::sync::{Arc, OnceLock};

// using Mutex to support tests that trying to initialize all at the same time
static INITIALIZED: OnceLock<Mutex<bool>> = OnceLock::new();

unsafe extern "C" {
    fn initialize_slatedbsqlite() -> i32;
}

pub fn init(db: Arc<Db>) {
    let _ = INITIALIZED.get_or_init(|| {
        tracing::info!("Initializing slatedbsqlite VFS...");
        let log_filename = Some("sqlite.log");
        set_vfs_context(db, log_filename);

        tracing::info!("slatedbsqlite VFS init start");
        let res = unsafe { initialize_slatedbsqlite() };
        tracing::info!("slatedbsqlite VFS init done: {res}");

        Mutex::new(true)
    });
}

pub async fn pragma_setup(connection: &Object) -> Result<()> {
    log::info!(logger: logger(), "pragmas_check");

    // Test VFS with pragma, if our vfs is loaded
    let vfs_detected = connection
        .interact(|conn| -> SqlResult<String> {
            let pragma_vfs = format!("PRAGMA {VFS_NAME:?}");
            let res = conn.query_row(&pragma_vfs, [], |row| row.get::<_, String>(0));
            if let Err(rusqlite::Error::QueryReturnedNoRows) = res {
                return Ok(String::new());
            }
            res
        })
        .await??;
    log::info!(logger: logger(), "vfs_detected={vfs_detected}");
    if vfs_detected != VFS_NAME.to_string_lossy() {
        return Err(NoVfsDetectedSnafu.fail()?);
    }

    // try enabling WAL (WAL not working yet)
    let _journal_mode = connection
        .interact(|conn| -> SqlResult<String> {
            conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))
        })
        .await??;
    log::info!(logger: logger(), "JOURNAL_MODE={_journal_mode:?}");

    // check if test table exists
    let check_res = connection
        .interact(|conn| -> SqlResult<Vec<String>> {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)",
                [],
            )?;
            let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type ='table'")?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .filter_map(SqlResult::ok)
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .await??;

    if !check_res.contains(&"test".to_string()) {
        tracing::error!("Didn't pass check, res={check_res:?}");
        return Err(SelfCheckSnafu.fail()?);
    } else {
        tracing::info!("VFS check passed");
    }

    Ok(())
}
