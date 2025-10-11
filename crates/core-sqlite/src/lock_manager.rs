#![allow(clippy::unwrap_used)]
use sqlite_plugin::flags;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::instrument;
use super::vfs::logger;

/// Manages SQLite-style hierarchical locking for files with multiple handles
#[derive(Clone)]
pub struct LockManager {
    // Map of file_path -> file lock state
    files: Arc<Mutex<HashMap<String, FileLockState>>>,
}

#[derive(Clone, Default)]
struct FileLockState {
    // Map of handle_id -> lock_level for this file
    handle_locks: HashMap<u64, flags::LockLevel>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire a lock on a file for a specific handle, blocking until available
    #[allow(clippy::cognitive_complexity)]
    pub fn lock(&self, file_path: &str, handle_id: u64, level: flags::LockLevel) -> Result<(), i32> {
        log::debug!(logger: logger(), "{file_path} lock request: level={level:?} handle_id={handle_id}");
        
        let mut files = self.files.lock().unwrap();

        // Get or create file lock state
        let file_state = files.entry(file_path.to_string())
            .or_insert_with(FileLockState::default)
            .clone();

        let max_lock_level = file_state.handle_locks.values()
            .map(|&level| Self::lock_level_to_u8(level))
            .max()
            .map(Self::u8_to_lock_level)
            .unwrap_or(flags::LockLevel::Unlocked);
        log::debug!(logger: logger(), "{file_path} lock - @lock max lock level={max_lock_level:?}");

        if !Self::is_lock_compatible(level, &file_state.handle_locks, handle_id) {
            return Err(sqlite_plugin::vars::SQLITE_BUSY);
        }

        // Wait for lock to become available, then acquire it
        let mut handle_locks = file_state.handle_locks;

        // Acquire the lock
        handle_locks.insert(handle_id, level);
        log::debug!(logger: logger(), "{file_path} lock acquired: level={level:?} handle_id={handle_id}");
        
        Ok(())
    }

    /// Release or downgrade a lock on a file for a specific handle
    #[allow(clippy::single_match_else, clippy::cognitive_complexity)]
    pub fn unlock(&self, file_path: &str, handle_id: u64, level: flags::LockLevel) -> Result<(), i32> {
        log::debug!(logger: logger(), "{file_path} lock - unlock request: level={level:?} handle_id={handle_id}");
        
        let mut files = self.files.lock().unwrap();

        // Get file lock state
        if let Some(file_state) = files.get_mut(file_path) {            
            match level {
                flags::LockLevel::Unlocked => {
                    // Completely unlock - remove this handle's lock
                    file_state.handle_locks.remove(&handle_id);
                    log::debug!(logger: logger(), "{file_path} lock removed: level={level:?} handle_id={handle_id}");
                }
                _ => {
                    // Downgrade to specified level
                    file_state.handle_locks.insert(handle_id, level);
                    log::debug!(logger: logger(), "{file_path} lock downgraded: level={level:?} handle_id={handle_id}");
                }
            }
        }

        Ok(())
    }

    /// Remove a handle entirely (called on file close)
    #[instrument(level = "error", skip(self), fields(file_state_removed))]
    #[allow(clippy::cognitive_complexity)]
    pub fn remove_handle(&self, file_path: &str, handle_id: u64) {
        log::debug!(logger: logger(), "removing handle: path={} handle_id={}", file_path, handle_id);
        
        let mut files = self.files.lock().unwrap();
        if let Some(file_state) = files.get_mut(file_path) {
            file_state.handle_locks.remove(&handle_id);

            // Check if file has any remaining handles
            if file_state.handle_locks.is_empty() {
                // Remove the entire file state if no handles remain
                files.remove(file_path);
                tracing::Span::current().record("file_state_removed", true);
                log::debug!(logger: logger(), "removed file state: path={file_path}");
            }
        }
    }

    // Get the current maximum lock level for a file (for diagnostics)
    pub fn get_max_lock_level(&self, file_path: &str) -> flags::LockLevel {
        let files = self.files.lock().unwrap();
        let max_lock_level = if let Some(file_state) = files.get(file_path) {
            file_state.handle_locks.values()
                .map(|&level| Self::lock_level_to_u8(level))
                .max()
                .map(Self::u8_to_lock_level)
                .unwrap_or(flags::LockLevel::Unlocked)
        } else {
            flags::LockLevel::Unlocked
        };
        log::debug!(logger: logger(), "{file_path} lock - get lock level={max_lock_level:?}");
        return max_lock_level;
    }

    pub fn get_max_lock_level_as_int(&self, file_path: &str) -> u8 {
        Self::lock_level_to_u8(
            self.get_max_lock_level(file_path)
        )
    }

    // Helper function to convert LockLevel to u8 for comparison
    fn lock_level_to_u8(level: flags::LockLevel) -> u8 {
        match level {
            flags::LockLevel::Unlocked => 0,
            flags::LockLevel::Shared => 1,
            flags::LockLevel::Reserved => 2,
            flags::LockLevel::Pending => 3,
            flags::LockLevel::Exclusive => 4,
        }
    }

    // Helper function to convert u8 back to LockLevel
    fn u8_to_lock_level(level: u8) -> flags::LockLevel {
        match level {
            0 => flags::LockLevel::Unlocked,
            1 => flags::LockLevel::Shared,
            2 => flags::LockLevel::Reserved,
            3 => flags::LockLevel::Pending,
            4 => flags::LockLevel::Exclusive,
            _ => flags::LockLevel::Unlocked,
        }
    }

    // Check if a lock level is compatible with existing locks
    #[allow(clippy::needless_continue)]
    #[allow(clippy::match_same_arms)]
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

        for (&existing_handle_id, &existing_level) in existing_locks {
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