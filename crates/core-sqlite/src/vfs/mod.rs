mod handle;
mod init;
mod lock_manager;

pub use init::{init, pragma_setup};

use parking_lot::Mutex;
use rusqlite::trace::config_log;
use slatedb::bytes::Bytes;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, WriteBatch};
use sqlite_plugin::flags;
use sqlite_plugin::vfs;
use sqlite_plugin::vfs::PragmaErr;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::io::Write;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
// mostly not using tracing instrument as it is not correctly initialized when used within connection
// and it just floods log
use chrono::Utc;
use tracing::instrument;

#[derive(Clone)]
struct Capabilities {
    atomic_batch: bool,
    point_in_time_reads: bool,
    sector_size: i32,
}

struct BatchWrite {
    offset: usize,
    data: Vec<u8>,
}

#[derive(Clone)]
struct FileState {
    pending_writes: Arc<Mutex<Vec<BatchWrite>>>,
    batch_open: Arc<AtomicBool>,
}

impl FileState {
    fn new() -> Self {
        Self {
            pending_writes: Arc::new(Mutex::new(Vec::new())),
            batch_open: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
struct SlatedbVfs {
    capabilities: Capabilities,
    db: Arc<Db>,
    sqlite_log: Arc<Mutex<Option<std::fs::File>>>,
    files: Arc<Mutex<HashMap<String, FileState>>>,
    handle_counter: Arc<AtomicU64>,
    lock_manager: lock_manager::LockManager,
}

pub const PAGE_SIZE: usize = 4096;

pub const VFS_NAME: &CStr = c"slatedb_vfs";

static VFS_INSTANCE: OnceLock<Arc<SlatedbVfs>> = OnceLock::new();

static LOGGER: OnceLock<Arc<dyn log::Log>> = OnceLock::new();

impl SlatedbVfs {
    pub fn new(db: Arc<Db>, sqlite_log: Option<std::fs::File>) -> Self {
        Self {
            db,
            sqlite_log: Arc::new(Mutex::new(sqlite_log)),
            files: Arc::new(Mutex::new(HashMap::new())),
            capabilities: Capabilities {
                atomic_batch: true,
                point_in_time_reads: true,
                sector_size: 4096,
            },
            handle_counter: Arc::new(AtomicU64::new(1)),
            lock_manager: lock_manager::LockManager::new(),
        }
    }

    // #[instrument(level = "error", skip(self, future))]
    fn block_on<F, T>(&self, future: F) -> Result<T, i32>
    where
        F: std::future::Future<Output = Result<T, i32>>,
    {
        // tokio::runtime::Handle::current().block_on(future)
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
    }

    // #[instrument(level = "error", skip(self, key, value))]
    pub async fn put<K, V>(&self, key: K, value: V) -> Result<(), i32>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        log::debug!(logger: logger(), "put: db::put key={:?}, value_len={:?}",
            String::from_utf8_lossy(key.as_ref()),
            value.as_ref().len(),
        );
        let res = self
            .db
            .put_with_options(
                key,
                value,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|e| {
                log::error!(logger: logger(), "error putting page: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_WRITE
            });
        log::debug!(logger: logger(), "put: db::put done");
        res
    }

    // #[instrument(level = "error", skip(self, key))]
    pub async fn delete<K>(&self, key: K) -> Result<(), i32>
    where
        K: AsRef<[u8]>,
    {
        log::debug!(logger: logger(), "delete: db::delete key={:?}", String::from_utf8_lossy(key.as_ref()));
        let res = self
            .db
            .delete_with_options(
                key,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|e| {
                log::error!(logger: logger(), "error deleting page: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_DELETE
            });
        log::debug!(logger: logger(), "delete: db::delete done");
        res
    }
    pub async fn db_write(&self, batch: WriteBatch) -> Result<(), i32> {
        log::debug!(logger: logger(), "db_write: db::write batch={:?}", batch);
        let res = self
            .db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|e| {
                log::error!(logger: logger(), "error writing page: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_WRITE
            });
        log::debug!(logger: logger(), "db_write: db::write done");
        res
    }

    // #[instrument(level = "error", skip(self, key))]
    pub async fn get<K>(&self, key: K) -> Result<Option<Bytes>, i32>
    where
        K: AsRef<[u8]> + Send,
    {
        log::debug!(logger: logger(), "get: db::get key={:?}", String::from_utf8_lossy(key.as_ref()));
        let res = self.db.get(key).await.map_err(|e| {
            log::error!(logger: logger(), "error getting page: {e}");
            sqlite_plugin::vars::SQLITE_IOERR_READ
        });
        log::debug!(logger: logger(), "get: db::get done");
        res
    }
}

impl vfs::Vfs for SlatedbVfs {
    type Handle = handle::SlatedbVfsHandle;

    #[instrument(level = "error", skip(self))]
    fn open(&self, path: Option<&str>, opts: flags::OpenOpts) -> vfs::VfsResult<Self::Handle> {
        let path = path.unwrap_or("");
        log::debug!(logger: logger(), "open: path={path}, opts={opts:?}");
        let mode = opts.mode();

        if mode.is_readonly() && !self.capabilities.point_in_time_reads {
            log::error!(logger: logger(), "read-only mode is not supported for this server");
            return Err(sqlite_plugin::vars::SQLITE_CANTOPEN);
        }

        if !path.is_empty() {
            self.block_on(async { self.put(&path, &[]).await })?;
        }

        let handle_id = self.handle_counter.fetch_add(1, Ordering::SeqCst);
        let handle = handle::SlatedbVfsHandle::new(path.to_string(), mode.is_readonly(), handle_id);
        log::debug!(logger: logger(), "open: done handle_id={handle_id}");
        Ok(handle)
    }

    // #[instrument(level = "error", skip(self))]
    fn delete(&self, path: &str) -> vfs::VfsResult<()> {
        log::debug!(logger: logger(), "delete: path={path}");

        self.block_on(async {
            // Delete all pages for this file
            let mut page_offset = 0;
            loop {
                let page_key = format!("{path}:page:{page_offset}");
                let exists = self.get(&page_key).await?;

                if exists.is_some() {
                    self.delete(&page_key).await?;
                    page_offset += PAGE_SIZE;
                } else {
                    break;
                }
            }
            self.delete(&path).await?;
            Ok::<(), i32>(())
        })?;

        Ok(())
    }

    // #[instrument(level = "error", skip(self), err)]
    fn access(&self, path: &str, flags: flags::AccessFlags) -> vfs::VfsResult<bool> {
        let exists = self.block_on(async { self.get(path).await })?.is_some();
        log::debug!(logger: logger(), "access: path={path}, flags={flags:?}, exists={exists}");
        Ok(exists)
    }

    // #[instrument(level = "error", skip(self, handle), fields(file = handle.path.as_str()), ret, err)]
    fn file_size(&self, handle: &mut Self::Handle) -> vfs::VfsResult<usize> {
        let max_size = self.block_on(async {
            // Find the highest page offset for this file to calculate total size
            // This is a simplified approach - in a real implementation you might want to
            // track file metadata separately for better performance
            let mut max_size = 0usize;

            // Check pages starting from 0 until we find no more
            let mut page_offset = 0;
            loop {
                let page_key = format!("{}:page:{}", handle.path, page_offset);
                let page_data = self.get(&page_key).await?;

                if let Some(page) = page_data {
                    max_size = page_offset + page.len();
                    page_offset += PAGE_SIZE;
                } else {
                    break;
                }
            }

            Ok::<usize, i32>(max_size)
        })?;

        Ok(max_size)
    }

    // #[instrument(level = "error", skip(self, handle))]
    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> vfs::VfsResult<()> {
        log::debug!(logger: logger(), "truncate: path={}, handle_id={}, size={size}", handle.path, handle.handle_id);
        if size == 0 {
            self.block_on(async { self.delete(handle.path.as_str()).await })?;
            return Ok(());
        }

        self.block_on(async {
            // Calculate which page contains the truncation point
            let truncate_page_offset = (size / PAGE_SIZE) * PAGE_SIZE;
            let truncate_offset_in_page = size % PAGE_SIZE;

            // Truncate the page that contains the truncation point
            let page_key = format!("{}:page:{}", handle.path, truncate_page_offset);
            let page_data = self.get(&page_key).await?;

            if let Some(page) = page_data {
                let mut page_vec = page.clone();
                if truncate_offset_in_page < page_vec.len() {
                    page_vec.truncate(truncate_offset_in_page);
                    self.put(&page_key, page_vec).await?;
                }
            }

            // Delete all pages beyond the truncation point
            let mut page_offset = truncate_page_offset + PAGE_SIZE;
            loop {
                let page_key = format!("{}:page:{}", handle.path, page_offset);
                let exists = self.get(&page_key).await?;

                if exists.is_some() {
                    self.delete(&page_key).await?;
                    page_offset += PAGE_SIZE;
                } else {
                    break;
                }
            }

            Ok::<(), i32>(())
        })?;

        Ok(())
    }

    // #[instrument(level = "error", skip(self, data))]
    fn write(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &[u8],
    ) -> vfs::VfsResult<usize> {
        // Get or create file state
        let file_state = {
            let mut files = self.files.lock();
            files
                .entry(handle.path.clone())
                .or_insert_with(FileState::new)
                .clone()
        };
        let is_batch_write = file_state.batch_open.load(Ordering::Acquire);
        log::debug!(logger: logger(),
            "write: path={}, offset={offset}, is_batch_write={is_batch_write}",
            handle.path
        );

        // Check if we're in batch mode for this file
        if is_batch_write {
            let mut pending_writes = file_state.pending_writes.lock();
            pending_writes.push(BatchWrite {
                offset,
                data: data.to_vec(),
            });
            // tracing::Span::current().record("pending_writes", pending_writes.len());
            return Ok(data.len());
        }

        // Write over the server
        self.block_on(async move {
            let page_offset = (offset / PAGE_SIZE) * PAGE_SIZE;
            let page_key = format!("{}:page:{}", handle.path, page_offset);

            // Get existing page data
            let existing_page = self.get(&page_key).await?;

            let mut page_data = if let Some(existing) = existing_page {
                existing.to_vec()
            } else {
                Vec::new()
            };

            let offset_in_page = offset % PAGE_SIZE;

            // Resize page if needed
            if offset_in_page + data.len() > page_data.len() {
                page_data.resize(offset_in_page + data.len(), 0);
            }

            log::debug!(logger: logger(),
                "write data at page {} offset {} length {}",
                page_offset,
                offset_in_page,
                data.len()
            );
            page_data[offset_in_page..offset_in_page + data.len()].copy_from_slice(data);

            self.put(&page_key, page_data).await
        })?;
        Ok(data.len())
    }

    // #[instrument(level = "error", skip(self, data))]
    #[allow(clippy::unwrap_used)]
    fn read(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &mut [u8],
    ) -> vfs::VfsResult<usize> {
        // Read from the server
        self.block_on(async move {
            // Calculate the page key using integer division
            let page_offset = (offset / PAGE_SIZE) * PAGE_SIZE;
            let page_key = format!("{}:page:{}", handle.path, page_offset);

            let page_data = self.get(&page_key).await?;

            if page_data.is_none() {
                log::debug!(logger: logger(), "read page not found, returning empty data");
                return Ok::<usize, i32>(0);
            }

            let page = page_data.unwrap();
            let offset_in_page = offset % PAGE_SIZE;

            // Check if offset is beyond page size
            if offset_in_page >= page.len() {
                log::debug!(logger: logger(), "read offset is beyond page size");
                return Ok(0);
            }

            // Read as much data as available from this page, up to the requested length
            let end_offset_in_page = std::cmp::min(offset_in_page + data.len(), page.len());
            let d = page[offset_in_page..end_offset_in_page].to_vec();

            log::debug!(logger: logger(), "read data length: {} from page {}", data.len(), page_offset);

            let len = data.len().min(d.len());
            data[..len].copy_from_slice(&d[..len]);
            Ok(len)
        })
    }

    #[instrument(level = "info", skip(self))]
    fn close(&self, handle: Self::Handle) -> vfs::VfsResult<()> {
        log::debug!(logger: logger(), "close: path={} handle_id={}", handle.path, handle.handle_id);

        // Remove handle from lock manager
        self.lock_manager
            .remove_handle(&handle.path, handle.handle_id);

        // Clean up file state if needed (keep for batch writes)
        // Note: We keep file states around for batch operations, lock manager handles its own cleanup

        Ok(())
    }

    fn device_characteristics(&self) -> i32 {
        log::debug!(logger: logger(), "device_characteristics");
        let mut characteristics: i32 = vfs::DEFAULT_DEVICE_CHARACTERISTICS;
        if self.capabilities.atomic_batch {
            characteristics |= sqlite_plugin::vars::SQLITE_IOCAP_BATCH_ATOMIC;
        }
        // TODO: Do we bother with SQLITE_IOCAP_IMMUTABLE if we're opened in read only mode?
        characteristics
    }

    #[instrument(level = "info", skip(self), ret)]
    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: vfs::Pragma<'_>,
    ) -> Result<Option<String>, vfs::PragmaErr> {
        let res = if pragma.name == VFS_NAME.to_string_lossy() {
            Ok(Some(pragma.name.to_string()))
        } else {
            Err(PragmaErr::NotFound)
        };
        log::info!(logger: logger(), "pragma: db_path={:?}, pragma={:?}, res={:?}", handle.path, pragma, res);
        res
    }

    // #[instrument(level = "error", skip(self, handle, op, _p_arg), fields(op_name, file = handle.path.as_str()), err)]
    fn file_control(
        &self,
        handle: &mut Self::Handle,
        op: c_int,
        _p_arg: *mut c_void,
    ) -> vfs::VfsResult<()> {
        let op_name = match op {
            sqlite_plugin::vars::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE => "begin_atomic_write",
            sqlite_plugin::vars::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE => "commit_atomic_write",
            sqlite_plugin::vars::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => "rollback_atomic_write",
            _ => "",
        };
        let op_name = if op_name.is_empty() {
            format!("{op:?}")
        } else {
            op_name.to_string()
        };
        // tracing::Span::current().record("op_name", op_name.as_str());
        log::debug!(logger: logger(), "file_control: file={:?}, op={op_name}", handle.path);
        match op {
            sqlite_plugin::vars::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE => {
                let file_state = {
                    let mut files = self.files.lock();
                    files
                        .entry(handle.path.clone())
                        .or_insert_with(FileState::new)
                        .clone()
                };
                // Open the write batch
                file_state.batch_open.store(true, Ordering::Release);
                Ok(())
            }
            sqlite_plugin::vars::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE => {
                let file_state = {
                    let mut files = self.files.lock();
                    files
                        .entry(handle.path.clone())
                        .or_insert_with(FileState::new)
                        .clone()
                };

                // Close the write batch
                file_state.batch_open.store(false, Ordering::Release);

                // Send the batch over the server
                self.block_on(async {
                    let batch = {
                        let mut pending = file_state.pending_writes.lock();
                        std::mem::take(&mut *pending)
                    };
                    if batch.is_empty() {
                        log::debug!(logger: logger(), "write batch is empty, nothing to commit");
                        return Ok(());
                    }
                    let mut page_writes: HashMap<usize, Vec<_>> = HashMap::new();
                    for write in &batch {
                        let offset = write.offset;
                        let page_offset = (offset / PAGE_SIZE) * PAGE_SIZE;

                        page_writes
                            .entry(page_offset)
                            .or_default()
                            .push((offset, write));
                    }
                    let db = self.db.clone();

                    // Prepare WriteBatch for atomic operation
                    let mut batch = WriteBatch::new();

                    // Apply writes to each affected page
                    for (page_offset, writes) in page_writes {
                        let page_key = format!("{}:page:{}", handle.path, page_offset);

                        // Get existing page data
                        let existing_page = db.get(&page_key).await.map_err(|e| {
                            log::error!(logger: logger(), "error getting page during atomic write: {e}");
                            sqlite_plugin::vars::SQLITE_IOERR_WRITE
                        })?;

                        let mut page_data = if let Some(existing) = existing_page {
                            existing.to_vec()
                        } else {
                            Vec::new()
                        };

                        // Apply all writes for this page
                        for (offset, write) in writes {
                            let offset_in_page = offset % PAGE_SIZE;

                            log::debug!(logger: logger(),
                                "atomic_write_batch write page={} offset_in_page={} length={}",
                                page_offset,
                                offset_in_page,
                                write.data.len(),
                            );

                            if offset_in_page + write.data.len() > page_data.len() {
                                page_data.resize(offset_in_page + write.data.len(), 0);
                            }
                            page_data[offset_in_page..offset_in_page + write.data.len()]
                                .copy_from_slice(&write.data);
                        }

                        // Add the page update to the batch
                        batch.put(&page_key, page_data);
                    }

                    // Execute all page updates atomically
                    self.db_write(batch).await
                })?;

                Ok(())
            }
            sqlite_plugin::vars::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => {
                let file_state = {
                    let mut files = self.files.lock();
                    files
                        .entry(handle.path.clone())
                        .or_insert_with(FileState::new)
                        .clone()
                };
                // Close the write batch
                file_state.batch_open.store(false, Ordering::Release);
                // Clear the batch
                file_state.pending_writes.lock().clear();
                Ok(())
            }
            _ => Err(sqlite_plugin::vars::SQLITE_NOTFOUND),
        }
    }

    fn sector_size(&self) -> i32 {
        log::debug!(logger: logger(), "sector_size");
        self.capabilities.sector_size
    }

    #[instrument(level = "debug", skip(self))]
    fn unlock(&self, handle: &mut Self::Handle, level: flags::LockLevel) -> vfs::VfsResult<()> {
        self.lock_manager
            .unlock(&handle.path, handle.handle_id, level)
    }
    #[instrument(level = "debug", skip(self))]
    fn lock(&self, handle: &mut Self::Handle, level: flags::LockLevel) -> vfs::VfsResult<()> {
        let res = self
            .lock_manager
            .lock(&handle.path, handle.handle_id, level);
        if res.is_err() {
            tracing::Span::current().record("rejected", true);
        }
        res
    }
    // #[instrument(level = "error", skip(self))]
    fn sync(&self, handle: &mut Self::Handle) -> vfs::VfsResult<()> {
        log::debug!(logger: logger(), "sync: db::flush path={}", handle.path);
        tokio::runtime::Handle::current().block_on(async {
            let db = self.db.clone();
            db.flush().await.map_err(|e| {
                log::error!(logger: logger(), "error flushing database: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_FSYNC
            })
        })?;
        Ok(())
    }
    #[instrument(level = "debug", skip(self), ret)]
    fn check_reserved_lock(&self, handle: &mut Self::Handle) -> vfs::VfsResult<i32> {
        let level = self.lock_manager.get_global_lock_level(&handle.path);
        if level >= flags::LockLevel::Reserved {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    fn register_logger(&self, logger: sqlite_plugin::logger::SqliteLogger) {
        pub struct LogCompat {
            logger: Mutex<sqlite_plugin::logger::SqliteLogger>,
        }

        impl log::Log for LogCompat {
            fn enabled(&self, _metadata: &log::Metadata) -> bool {
                true
            }

            fn log(&self, record: &log::Record) {
                let level = record.level();
                let args = record.args();
                let target = record.target();
                let file = record.file();
                let line = record.line();
                let location = file
                    .map(|f| format!("log::{}:{}", f, line.unwrap_or_default()))
                    .unwrap_or_default();

                let trace_msg = format!("{level} {target}: {location}: {args}");
                let level = match record.level() {
                    log::Level::Error => {
                        tracing::error!("{trace_msg}");
                        sqlite_plugin::logger::SqliteLogLevel::Error
                    }
                    log::Level::Warn => {
                        // tracing::warn!("{trace_msg}");
                        sqlite_plugin::logger::SqliteLogLevel::Warn
                    }
                    _ => {
                        // tracing::info!("{trace_msg}");
                        sqlite_plugin::logger::SqliteLogLevel::Notice
                    }
                };

                let msg = format!("{}", record.args());

                // send to native sqlite log
                self.logger.lock().log(level, msg.as_bytes());
            }

            fn flush(&self) {
                // println!("flush");
            }
        }

        tracing::debug!("Setting VFS logger");
        if let Err(_) = LOGGER.set(Arc::new(LogCompat {
            logger: Mutex::new(logger),
        })) {
            tracing::debug!("Use existing VFS logger");
        }

        // set the log level to trace
        log::set_max_level(log::LevelFilter::Trace);
    }
}

pub fn logger() -> Arc<dyn log::Log> {
    LOGGER.get().unwrap().clone()
}

pub fn set_vfs_context(db: Arc<Db>, log_file: Option<&str>) {
    let file = if let Some(log_file) = log_file {
        create_sqlite_log_file(log_file)
    } else {
        None
    };
    // allowed to init only once
    let _ = VFS_INSTANCE.get_or_init(|| Arc::new(SlatedbVfs::new(db, file)));
}

#[allow(clippy::expect_used)]
fn get_vfs() -> Arc<SlatedbVfs> {
    VFS_INSTANCE
        .get()
        .expect("VFS_INSTANCE is not initialized")
        .clone()
}

fn create_sqlite_log_file(path: &str) -> Option<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .ok()
}

fn sqlite_log_callback(err_code: std::ffi::c_int, msg: &str) {
    let vfs = get_vfs();
    let mut log = vfs.sqlite_log.lock();
    let time = Utc::now().format("%Y-%m-%d %H:%M:%S:%3f");
    let thread_id = std::thread::current().id();
    let code = match err_code {
        sqlite_plugin::vars::SQLITE_OK => "OK",
        sqlite_plugin::vars::SQLITE_ERROR => "ERROR",
        sqlite_plugin::vars::SQLITE_WARNING => "WARNING",
        sqlite_plugin::vars::SQLITE_NOTICE => "NOTICE",
        sqlite_plugin::vars::SQLITE_INTERNAL => "INTERNAL",
        _ => &format!("SQlite code={err_code}"),
    };

    let fmt = format_args!("{code} [{time}] {thread_id:?} {msg}\n");
    if let Some(file) = log.as_mut() {
        let _ = file.write_fmt(fmt);
    } else {
        eprintln!("{}", fmt);
    }
}

///  This function initializes the VFS statically.
/// Called automatically when the library is loaded.
///
/// # Safety
/// This function is safe to call from C as it only registers a VFS implementation
/// with `SQLite` and doesn't access any raw pointers or perform unsafe operations.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn initialize_slatedbsqlite() -> i32 {
    let vfs = get_vfs();

    if let Err(err) = vfs::register_static(
        VFS_NAME.to_owned(),
        (*vfs).clone(),
        vfs::RegisterOpts { make_default: true },
    ) {
        // not using log::error as it is not initialized yet
        tracing::error!("Failed to initialize slatedbsqlite: {err}");
        return err;
    }

    // setup internal sqlite log
    if let Err(err) = unsafe { config_log(Some(sqlite_log_callback)) } {
        // not using log::error as it is not initialized yet
        tracing::error!("Failed to set sqlite log callback: {err}");
    }

    sqlite_plugin::vars::SQLITE_OK
}

/// This function is called by `SQLite` when the extension is loaded. It registers
/// the memvfs VFS with `SQLite`.
///
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
/// The provided pointers must be valid `SQLite` API structures.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_slatedbsqlite_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_char,
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
) -> std::os::raw::c_int {
    let vfs = get_vfs();
    if let Err(err) = unsafe {
        vfs::register_dynamic(
            p_api,
            VFS_NAME.to_owned(),
            (*vfs).clone(),
            vfs::RegisterOpts { make_default: true },
        )
    } {
        return err;
    }

    sqlite_plugin::vars::SQLITE_OK_LOAD_PERMANENTLY
}
