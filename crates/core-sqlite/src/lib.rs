mod lock_manager;
mod handle;
mod vfs;
mod env_config;

use tokio::runtime::Handle;
use slatedb::Db;
use rusqlite::{Connection};
use std::sync::Arc;

unsafe extern "C" {
    fn initialize_grpsqlite() -> i32;
}

pub struct SqliteStore {
    pub connection: Connection,
    pub current_db: String,
}

impl SqliteStore {
    pub fn init(db: Arc<Db>) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let runtime = Handle::current();
        vfs::set_vfs_context(runtime, db);

        // Initialize grpsqlite VFS
        println!("Initializing grpsqlite VFS...");
        unsafe { initialize_grpsqlite() };

        // Open database connection
        let connection = Connection::open("embucket.db")?;

        // Test VFS with pragma
        let mut vfs_detected = false;
        if let Ok(mut stmt) = connection.prepare("PRAGMA is_memory_server") {
            let mut rows = stmt.query([])?;
            if let Ok(Some(row)) = rows.next() {
                if let Ok(result) = row.get::<usize, String>(0) {
                    println!("result: {result}");
                    vfs_detected = result == "maybe?";
                }
            }
        }

        if !vfs_detected {
            return Err("grpsqlite VFS not detected".into());
        }

        Ok(Self {
            connection: connection,
            current_db: "embucket.db".to_string(),
        })
    }
}