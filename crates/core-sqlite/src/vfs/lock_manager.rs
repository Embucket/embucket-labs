#![allow(clippy::unwrap_used)]
use sqlite_plugin::flags::LockLevel;
use sqlite_plugin::vars::SQLITE_BUSY;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use super::logger;


/// Manages SQLite-style hierarchical locking for files with multiple handles
#[derive(Clone)]
pub struct LockManager {
    // Map of file_path -> file lock state
    files: Arc<Mutex<HashMap<String, VfsFileState>>>,
}

#[derive(Clone, Debug)]
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
    pub fn lock(&mut self, handle_id: u64, new_lock: LockLevel) -> Result<(), i32> {
        let res = match (self.global_lock, new_lock) {
            (LockLevel::Unlocked, _) => {
                // upgrade Unlocked to any lock
                self.handles.insert(handle_id, new_lock);
                self.global_lock = new_lock;
                Ok(())
            }
            (LockLevel::Shared, LockLevel::Shared) => {
                // allow acquire multiple Shared locks
                self.handles.insert(handle_id, new_lock);
                Ok(())
            }
            (_, LockLevel::Reserved) => {
                if self.global_lock == LockLevel::Shared {
                    self.handles.insert(handle_id, new_lock);
                    self.global_lock = new_lock;
                    Ok(())
                } else {
                    Err(SQLITE_BUSY)
                }
            }
            (LockLevel::Reserved, LockLevel::Shared) => {
                // allow acquire new Shared lock, do not change global lock
                self.handles.insert(handle_id, new_lock);
                Ok(())
            }
            (LockLevel::Pending, LockLevel::Shared) => {
                Err(SQLITE_BUSY)
            }
            (_, LockLevel::Exclusive) => {
                // need to know only locks other than this handle and non unlock
                let other_locks_count = self.handles
                    .iter()
                    .filter(|h| h.0 != &handle_id && h.1 != &LockLevel::Unlocked)
                    .count();
                if other_locks_count > 0 {
                    Err(SQLITE_BUSY)
                } else {
                    self.handles.insert(handle_id, new_lock);
                    self.global_lock = new_lock;
                    Ok(())
                }
            }
            (LockLevel::Exclusive, _) => {
                // no locks can acquire while Exclusive is held
                Err(SQLITE_BUSY)
            }
            _ => {
                Ok(())
            }
        };
        res
    }

    pub fn unlock(&mut self, handle_id: u64, level: LockLevel) -> Result<(), i32> {
        if self.global_lock > level && (level == LockLevel::Unlocked || level == LockLevel::Shared) {
            if level == LockLevel::Unlocked {
                self.handles.remove(&handle_id);
            } else {
                self.handles.insert(handle_id, level);
            }
            self.global_lock = self.max_lock();
        }
        Ok(())
    }

    pub fn max_lock(&self) -> LockLevel {
        self.handles
            .iter()
            .map(|lock| *lock.1)
            .max()
            .unwrap_or(LockLevel::Unlocked)
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
        
        {
            let mut files = self.files.lock();

            // Get or create file lock state
            let file_state = files
                .entry(file_path.to_string())
                .or_insert_with(VfsFileState::default);

            let lock_before = file_state.global_lock;

            // return error immediately if lock is not acquired
            file_state.lock(handle_id, level)?;

            log::debug!(logger: logger(), 
                "{file_path} lock acquired {lock_before:?}->{level:?}(global={:?}) handle_id={handle_id}, {:?}", 
                file_state.global_lock, file_state.handles
            );
        }

        {
            let files = self.files.lock();
            log::debug!(logger: logger(), "{file_path} lock after handle_id={handle_id}, {files:?}");
        }
        
        Ok(())
    }

    /// Release or downgrade a lock on a file for a specific handle
    #[allow(clippy::single_match_else, clippy::cognitive_complexity)]
    pub fn unlock(&self, file_path: &str, handle_id: u64, level: LockLevel) -> Result<(), i32> {
        log::debug!(logger: logger(), "{file_path} lock - unlock request: level={level:?} handle_id={handle_id}");
        
        let mut files = self.files.lock();

        // Get file lock state
        if let Some(file_state) = files.get_mut(file_path) {
            let lock_before = file_state.global_lock;

            // return error immediately if lock is not released
            file_state.unlock(handle_id, level)?;
            let global_lock = file_state.global_lock;

            log::debug!(logger: logger(),
                "{file_path} lock - released: {lock_before:?}->{level:?} (global={global_lock:?}) handle_id={handle_id}, {:?}",
                file_state.handles
            );
        }

        Ok(())
    }

    /// Remove a handle entirely (called on file close)
    pub fn remove_handle(&self, file_path: &str, handle_id: u64) {
        log::debug!(logger: logger(), "remove_handle: path={} handle_id={}", file_path, handle_id);
        
        let mut files = self.files.lock();
        if let Some(file_state) = files.get_mut(file_path) {
            if file_state.handles.get(&handle_id) == Some(&LockLevel::Unlocked) {
                file_state.handles.remove(&handle_id);
                if file_state.handles.is_empty() {
                    files.remove(file_path);
                    log::debug!(logger: logger(), "removed file state: path={file_path}");
                } else {
                    file_state.global_lock = file_state
                        .handles
                        .iter()
                        .map(|lock| *lock.1)
                        .max()
                        .unwrap_or(LockLevel::Unlocked);
                }
            } else {
                log::debug!(logger: logger(),
                    "for path={file_path} remained opened handles: {:?}", file_state.handles.keys()
                );
            }
        }
        log::debug!(logger: logger(), "remove_handle: done");
    }

    pub fn get_global_lock_level(&self, file_path: &str) -> LockLevel {
        let files = self.files.lock();
        let global_lock_level = if let Some(file_state) = files.get(file_path) {
            file_state.global_lock
        } else {
            LockLevel::Unlocked
        };
        log::debug!(logger: logger(), "{file_path} global lock level={global_lock_level:?}");
        return global_lock_level;
    }
}