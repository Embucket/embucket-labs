#![allow(clippy::unwrap_used)]
use sqlite_plugin::flags::LockLevel;
use sqlite_plugin::vars::SQLITE_BUSY;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::instrument;
use super::vfs::logger;


/// Manages SQLite-style hierarchical locking for files with multiple handles
#[derive(Clone)]
pub struct LockManager {
    // Map of file_path -> file lock state
    files: Arc<Mutex<HashMap<String, VfsFileState>>>,
}

#[derive(Clone)]
struct VfsFileState {
    global_lock: LockLevel,
    handles: HashMap<u64, LockLevel>,
}

impl Default for VfsFileState {
    fn default() -> Self {
        Self {
            global_lock: LockLevel::Unlocked,
            handles: HashMap::new(),
        }
    }
}

impl VfsFileState {
    pub fn set_lock_level(&mut self, handle_id: u64, level: LockLevel) -> Result<(), i32> {
        let current_level = *self.handles.get(&handle_id).unwrap_or(&LockLevel::default());

        match (current_level, level) {
            // -------------------------------
            // Release locks
            // -------------------------------
            (_, LockLevel::Unlocked) => {
                self.handles.remove(&handle_id);

                if current_level == LockLevel::Shared {
                    if !self.handles.values().any(|&lock| lock == LockLevel::Shared) {
                        self.global_lock = LockLevel::Unlocked;
                    }
                } else {
                    if self.handles.is_empty() {
                        self.global_lock = LockLevel::Unlocked;
                    }
                }
            }

            // -------------------------------
            // xLock() or xUnlock()
            // Acquire Shared
            // -------------------------------
            (_, LockLevel::Shared) => {
                // Shared locks cannot coexist with Reserved or Exclusive
                if self.global_lock > LockLevel::Shared {
                    return Err(SQLITE_BUSY);
                }
            }

            // -------------------------------
            // Acquire Reserved
            // -------------------------------
            (_, LockLevel::Reserved) => {
                // Only one handle can have Reserved, and only if file not Exclusive
                if self.global_lock == LockLevel::Exclusive
                    || self
                        .handles
                        .iter()
                        .any(|(&id, &lvl)| id != handle_id && lvl >= LockLevel::Reserved)
                {
                    return Err(SQLITE_BUSY);
                }
            }

            // -------------------------------
            // Acquire Exclusive
            // -------------------------------
            (_, LockLevel::Exclusive) => {
                // Must be the only active handle
                if self
                    .handles
                    .iter()
                    .any(|(&id, &lvl)| id != handle_id && lvl != LockLevel::Unlocked)
                {
                    return Err(SQLITE_BUSY);
                }
            }

            // -------------------------------
            // Pending (rarely used; transitional)
            // -------------------------------
            (_, LockLevel::Pending) => {
                // Can transition from Reserved to Pending
                if current_level < LockLevel::Reserved {
                    return Err(SQLITE_BUSY);
                }
            }
        };
        if level != LockLevel::Unlocked {
            self.handles.insert(handle_id, level);
            self.global_lock = level;
        }
        Ok(())
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire a lock on a file for a specific handle, blocking until available
    #[allow(clippy::cognitive_complexity)]
    pub fn lock(&self, file_path: &str, handle_id: u64, level: LockLevel) -> Result<(), i32> {
        log::debug!(logger: logger(), "{file_path} lock request: level={level:?} handle_id={handle_id}");
        
        let mut files = self.files.lock().unwrap();

        // Get or create file lock state
        let mut file_state = files
            .entry(file_path.to_string())
            .or_insert_with(VfsFileState::default)
            .clone();

        file_state.set_lock_level(handle_id, level)?;

        let global_lock = file_state.global_lock;

        log::debug!(logger: logger(), 
            "{file_path} lock acquired={level:?} global_lock={global_lock:?} handle_id={handle_id}", 
        );
        
        Ok(())
    }

    /// Release or downgrade a lock on a file for a specific handle
    #[allow(clippy::single_match_else, clippy::cognitive_complexity)]
    pub fn unlock(&self, file_path: &str, handle_id: u64, level: LockLevel) -> Result<(), i32> {
        log::debug!(logger: logger(), "{file_path} lock - unlock request: level={level:?} handle_id={handle_id}");
        
        let mut files = self.files.lock().unwrap();

        // Get file lock state
        if let Some(file_state) = files.get_mut(file_path) {
            file_state.set_lock_level(handle_id, level)?;
            let global_lock = file_state.global_lock;

            log::debug!(logger: logger(),
                "{file_path} lock - unlocked: level={level:?} global_lock={global_lock:?} handle_id={handle_id}"
            );
        }

        Ok(())
    }

    /// Remove a handle entirely (called on file close)
    #[instrument(level = "error", skip(self), fields(file_state_removed))]
    pub fn remove_handle(&self, file_path: &str, handle_id: u64) {
        log::debug!(logger: logger(), "remove_handle: path={} handle_id={}", file_path, handle_id);
        
        let mut files = self.files.lock().unwrap();
        if let Some(file_state) = files.get_mut(file_path) {
            // following unlock just a check before deleting file handle
            if let Ok(_) = file_state.set_lock_level(handle_id, LockLevel::Unlocked) {
                if file_state.global_lock == LockLevel::Unlocked {
                    file_state.handles.remove(&handle_id);
                    if file_state.handles.is_empty() {
                        files.remove(file_path);
                        log::debug!(logger: logger(), "removed file state: path={file_path}");
                    }
                }
            } else {
                log::debug!(logger: logger(),
                    "for path={file_path} remained opened handles: {:?}", file_state.handles.keys()
                );
            }
        }
        log::debug!(logger: logger(), "remove_handle: done");
    }

    // Get the current maximum lock level for a file (for diagnostics)
    pub fn get_global_lock_level(&self, file_path: &str) -> LockLevel {
        let files = self.files.lock().unwrap();
        let global_lock_level = if let Some(file_state) = files.get(file_path) {
            file_state.global_lock
        } else {
            LockLevel::Unlocked
        };
        log::debug!(logger: logger(), "{file_path} global lock level={global_lock_level:?}");
        return global_lock_level;
    }
}