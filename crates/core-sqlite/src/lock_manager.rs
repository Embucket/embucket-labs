#![allow(clippy::unwrap_used)]
use sqlite_plugin::flags;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use tracing::{debug, instrument};

/// Manages SQLite-style hierarchical locking for files with multiple handles
#[derive(Clone)]
pub struct LockManager {
    // Map of file_path -> file lock state
    files: Arc<Mutex<HashMap<String, FileLockState>>>,
}

#[derive(Clone)]
struct FileLockState {
    // Map of handle_id -> lock_level for this file
    handle_locks: Arc<Mutex<HashMap<u64, flags::LockLevel>>>,
    // Condition variable to notify waiting lock requests
    lock_condvar: Arc<Condvar>,
}

impl FileLockState {
    fn new() -> Self {
        Self {
            handle_locks: Arc::new(Mutex::new(HashMap::new())),
            lock_condvar: Arc::new(Condvar::new()),
        }
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire a lock on a file for a specific handle, blocking until available
    #[instrument(level = "debug", skip(self))]
    #[allow(clippy::cognitive_complexity)]
    pub fn lock(&self, file_path: &str, handle_id: u64, level: flags::LockLevel) -> Result<(), i32> {
        debug!("lock request: path={} handle_id={} level={:?}", file_path, handle_id, level);
        
        // Get or create file lock state
        let file_state = {
            let mut files = self.files.lock().unwrap();
            files.entry(file_path.to_string())
                .or_insert_with(FileLockState::new)
                .clone()
        };

        // Wait for lock to become available, then acquire it
        let mut handle_locks = file_state.handle_locks.lock().unwrap();
        
        // Wait until the lock is compatible
        while !Self::is_lock_compatible(level, &handle_locks, handle_id) {
            debug!("lock waiting: path={} handle_id={} level={:?}", file_path, handle_id, level);
            handle_locks = file_state.lock_condvar.wait(handle_locks).unwrap();
        }

        // Acquire the lock
        handle_locks.insert(handle_id, level);
        debug!("lock acquired: path={} handle_id={} level={:?}", file_path, handle_id, level);
        
        Ok(())
    }

    /// Release or downgrade a lock on a file for a specific handle
    #[instrument(level = "debug", skip(self))]
    pub fn unlock(&self, file_path: &str, handle_id: u64, level: flags::LockLevel) -> Result<(), i32> {
        debug!("unlock request: path={} handle_id={} level={:?}", file_path, handle_id, level);
        
        // Get file lock state
        let file_state = {
            let files = self.files.lock().unwrap();
            files.get(file_path).cloned()
        };

        if let Some(file_state) = file_state {
            let mut handle_locks = file_state.handle_locks.lock().unwrap();
            
            match level {
                flags::LockLevel::Unlocked => {
                    // Completely unlock - remove this handle's lock
                    handle_locks.remove(&handle_id);
                    debug!("lock removed: path={} handle_id={}", file_path, handle_id);
                }
                _ => {
                    // Downgrade to specified level
                    handle_locks.insert(handle_id, level);
                    debug!("lock downgraded: path={} handle_id={} to level={:?}", file_path, handle_id, level);
                }
            }

            // Notify any waiting threads that lock state has changed
            file_state.lock_condvar.notify_all();
            debug!("lock waiters notified: path={}", file_path);
        }

        Ok(())
    }

    /// Remove a handle entirely (called on file close)
    #[instrument(level = "debug", skip(self))]
    pub fn remove_handle(&self, file_path: &str, handle_id: u64) {
        debug!("removing handle: path={} handle_id={}", file_path, handle_id);
        
        let should_remove_file = {
            let files = self.files.lock().unwrap();
            if let Some(file_state) = files.get(file_path) {
                let mut handle_locks = file_state.handle_locks.lock().unwrap();
                handle_locks.remove(&handle_id);
                
                // Notify waiters in case this was blocking someone
                file_state.lock_condvar.notify_all();
                
                // Check if file has any remaining handles
                let should_remove = handle_locks.is_empty();
                drop(handle_locks);
                should_remove
            } else {
                false
            }
        };

        // Remove the entire file state if no handles remain
        if should_remove_file {
            let mut files = self.files.lock().unwrap();
            files.remove(file_path);
            debug!("removed file state: path={}", file_path);
        }
    }

    // Get the current maximum lock level for a file (for diagnostics)
    // pub fn get_max_lock_level(&self, file_path: &str) -> flags::LockLevel {
    //     let files = self.files.lock().unwrap();
    //     if let Some(file_state) = files.get(file_path) {
    //         let handle_locks = file_state.handle_locks.lock().unwrap();
    //         handle_locks.values()
    //             .map(|&level| Self::lock_level_to_u8(level))
    //             .max()
    //             .map(Self::u8_to_lock_level)
    //             .unwrap_or(flags::LockLevel::Unlocked)
    //     } else {
    //         flags::LockLevel::Unlocked
    //     }
    // }

    // Helper function to convert LockLevel to u8 for comparison
    // fn lock_level_to_u8(level: flags::LockLevel) -> u8 {
    //     match level {
    //         flags::LockLevel::Unlocked => 0,
    //         flags::LockLevel::Shared => 1,
    //         flags::LockLevel::Reserved => 2,
    //         flags::LockLevel::Pending => 3,
    //         flags::LockLevel::Exclusive => 4,
    //     }
    // }

    // Helper function to convert u8 back to LockLevel
    // fn u8_to_lock_level(level: u8) -> flags::LockLevel {
    //     match level {
    //         0 => flags::LockLevel::Unlocked,
    //         1 => flags::LockLevel::Shared,
    //         2 => flags::LockLevel::Reserved,
    //         3 => flags::LockLevel::Pending,
    //         4 => flags::LockLevel::Exclusive,
    //         _ => flags::LockLevel::Unlocked,
    //     }
    // }

    // Check if a lock level is compatible with existing locks
    #[allow(clippy::needless_continue)]
    fn is_lock_compatible(
        requested: flags::LockLevel,
        existing_locks: &HashMap<u64, flags::LockLevel>,
        handle_id: u64,
    ) -> bool {
        // SQLite locking rules:
        // - Multiple SHARED locks are allowed
        // - Only one RESERVED, PENDING, or EXCLUSIVE lock is allowed
        // - EXCLUSIVE lock excludes all other locks
        // - A handle can always upgrade its own lock

        for (&existing_handle_id, &existing_level) in existing_locks.iter() {
            // Skip our own handle - we can always upgrade our own lock
            if existing_handle_id == handle_id {
                continue;
            }

            match (requested, existing_level) {
                // Can't have EXCLUSIVE with any other lock
                (flags::LockLevel::Exclusive, _) | (_, flags::LockLevel::Exclusive) => {
                    return false;
                }
                // Can't have PENDING with RESERVED or PENDING
                (flags::LockLevel::Pending, flags::LockLevel::Reserved) => return false,
                (flags::LockLevel::Pending, flags::LockLevel::Pending) => return false,
                (flags::LockLevel::Reserved, flags::LockLevel::Pending) => return false,
                // Can't have multiple RESERVED locks
                (flags::LockLevel::Reserved, flags::LockLevel::Reserved) => return false,
                // SHARED with SHARED is OK, everything else with UNLOCKED is OK
                _ => continue,
            }
        }
        true
    }
}