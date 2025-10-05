mod lock_manager;
mod handle;
mod vfs;
mod sqlite_config;
mod error;

use tokio::runtime::Handle;
use slatedb::Db;
use std::sync::{Arc};
use error::{self as sqlite_error, Result};
use snafu::{ResultExt, OptionExt};
use std::sync::Mutex;
use std::collections::HashMap;
use std::sync::OnceLock;
use r2d2_sqlite::SqliteConnectionManager;
use r2d2::{PooledConnection, Pool};

const DEFAULT_DB_NAME: &str = "embucket.db";

unsafe extern "C" {
    fn initialize_grpsqlite() -> i32;
}

pub struct SqliteStore {
    connections: Mutex<HashMap<String, r2d2::Pool<SqliteConnectionManager>>>,
}

impl SqliteStore {
    pub fn conn(&self, db_name: &str) -> Result<PooledConnection<SqliteConnectionManager>> {
        if let Ok(mut connections) = self.connections.lock() {
            if let Some(conn) = connections.get(db_name) {
                Ok(conn.get().context(sqlite_error::R2d2Snafu)?)
            } else {
                let manager = SqliteConnectionManager::file(db_name);
                let pool = Pool::new(manager)
                    .context(sqlite_error::R2d2Snafu)?;
                let conn = pool.get()
                    .context(sqlite_error::R2d2Snafu)?;
                connections.insert(db_name.to_string(), pool);
                Ok(conn)
            }
        } else {
            sqlite_error::ConnectionsLockSnafu.fail()?
        }
    }

    pub fn default_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>> {
        self.conn(DEFAULT_DB_NAME)
    }

    #[tracing::instrument(name = "SqliteStore::init", skip(db), err)]
    pub fn init(db: Arc<Db>) -> Result<Arc<Self>> {
        let runtime = Handle::current();
        vfs::set_vfs_context(runtime, db);

        // Initialize grpsqlite VFS
        tracing::debug!("Initializing grpsqlite VFS...");
        unsafe { initialize_grpsqlite() };

        let sqlite_store = SqliteStore {
            connections: Mutex::new(HashMap::new()),
        };

        // Open database connection
        let connection = sqlite_store.default_conn()?;

        // Test VFS with pragma
        let mut vfs_detected = false;
        if let Ok(mut stmt) = connection.prepare("PRAGMA is_memory_server") {
            let mut rows = stmt.query([])
                .context(sqlite_error::RusqliteSnafu)?;
            if let Ok(Some(row)) = rows.next() {
                if let Ok(result) = row.get::<usize, String>(0) {
                    println!("result: {result}");
                    vfs_detected = result == "maybe?";
                }
            }
        }

        if !vfs_detected {
            return Err(sqlite_error::NoVfsDetectedSnafu.fail()?)
        }

        Ok(Arc::new(sqlite_store))
    }
}