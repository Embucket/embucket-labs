use super::lock_manager;
use super::handle;
use parking_lot::Mutex;
use slatedb::bytes::Bytes;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, WriteBatch};
use sqlite_plugin::flags;
use sqlite_plugin::vfs;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::runtime::Handle;
use tracing::{Level, instrument, span};
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{Registry, layer::SubscriberExt};

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
struct GrpcVfs {
    runtime: Arc<tokio::runtime::Handle>,
    capabilities: Capabilities,
    db: Arc<Db>,
    files: Arc<Mutex<HashMap<String, FileState>>>,
    _guard: Arc<Mutex<Option<tracing_chrome::FlushGuard>>>,
    handle_counter: Arc<AtomicU64>,
    lock_manager: lock_manager::LockManager,
}

const PAGE_SIZE: usize = 4096;

impl GrpcVfs {
    pub fn new(runtime: Handle, db: Arc<Db>) -> Self {
        let guard = setup_tracing();

        Self {
            db,
            runtime: Arc::new(runtime),
            files: Arc::new(Mutex::new(HashMap::new())),
            capabilities: Capabilities {
                atomic_batch: true,
                point_in_time_reads: false,
                sector_size: 4096,
            },
            _guard: Arc::new(Mutex::new(Some(guard))),
            handle_counter: Arc::new(AtomicU64::new(1)),
            lock_manager: lock_manager::LockManager::new(),
        }
    }

    fn block_on<F, T>(&self, future: F) -> Result<T, i32>
    where
        F: std::future::Future<Output = Result<T, i32>>,
    {
        let span = span!(Level::INFO, "block_on");
        let _guard = span.enter();
        tokio::task::block_in_place(|| self.runtime.block_on(future))
    }

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<(), i32>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let span = span!(Level::INFO, "put");
        let _guard = span.enter();
        self.db
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
                log::error!("error putting page: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_WRITE
            })
    }

    pub async fn delete<K>(&self, key: K) -> Result<(), i32>
    where
        K: AsRef<[u8]>,
    {
        let span = span!(Level::INFO, "delete");
        let _guard = span.enter();
        self.db
            .delete_with_options(
                key,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|e| {
                log::error!("error deleting page: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_DELETE
            })
    }
    pub async fn db_write(&self, batch: WriteBatch) -> Result<(), i32> {
        let span = span!(Level::INFO, "db_write");
        let _guard = span.enter();
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|e| {
                log::error!("error writing page: {e}");
                sqlite_plugin::vars::SQLITE_IOERR_WRITE
            })
    }


    pub async fn get<K>(&self, key: K) -> Result<Option<Bytes>, i32>
    where
        K: AsRef<[u8]> + Send,
    {
        let span = span!(Level::INFO, "get");
        let _guard = span.enter();
        self.db.get(key).await.map_err(|e| {
            log::error!("error getting page: {e}");
            sqlite_plugin::vars::SQLITE_IOERR_READ
        })
    }
}

impl vfs::Vfs for GrpcVfs {
    type Handle = handle::GrpcVfsHandle;

    fn register_logger(&self, logger: sqlite_plugin::logger::SqliteLogger) {
        struct LogCompat {
            logger: Mutex<sqlite_plugin::logger::SqliteLogger>,
        }

        impl log::Log for LogCompat {
            fn enabled(&self, _metadata: &log::Metadata) -> bool {
                true
            }

            fn log(&self, record: &log::Record) {
                let level = match record.level() {
                    log::Level::Error => sqlite_plugin::logger::SqliteLogLevel::Error,
                    log::Level::Warn => sqlite_plugin::logger::SqliteLogLevel::Warn,
                    _ => sqlite_plugin::logger::SqliteLogLevel::Notice,
                };
                if !record.target().contains("s3qlite") {
                    // Filter out logs from other modules.
                    return;
                }
                let msg = format!("{}", record.args());
                tracing::debug!("sqlite: {msg}");
                self.logger.lock().log(level, msg.as_bytes());
            }

            fn flush(&self) {
                tracing::debug!("sqlite: flush");
            }
        }

        let log = LogCompat {
            logger: Mutex::new(logger),
        };
        if let Err(e) = log::set_boxed_logger(Box::new(log)) {
            // Logger already set, ignore the error
            tracing::error!("Logger already initialized: {e}");
        }
    }

    #[instrument(level = "info", skip(self, path, opts))]
    fn open(&self, path: Option<&str>, opts: flags::OpenOpts) -> vfs::VfsResult<Self::Handle> {
        let path = path.unwrap_or("");
        log::debug!("open: path={path}, opts={opts:?}");
        let mode = opts.mode();

        if mode.is_readonly() && !self.capabilities.point_in_time_reads {
            log::error!("read-only mode is not supported for this server");
            return Err(sqlite_plugin::vars::SQLITE_CANTOPEN);
        }

        if !path.is_empty() {
            self.block_on(async { self.put(&path, &[]).await })?;
        }

        let handle_id = self.handle_counter.fetch_add(1, Ordering::SeqCst);
        let handle = handle::GrpcVfsHandle::new(path.to_string(), mode.is_readonly(), handle_id);
        Ok(handle)
    }

    #[instrument(level = "info", skip(self))]
    fn delete(&self, path: &str) -> vfs::VfsResult<()> {
        log::debug!("delete: path={path}");

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

    #[instrument(level = "info", skip(self, path, flags))]
    fn access(&self, path: &str, flags: flags::AccessFlags) -> vfs::VfsResult<bool> {
        let exists = self.block_on(async { self.get(path).await })?.is_some();
        log::debug!("access: path={path}, flags={flags:?}, exists={exists}");
        Ok(exists)
    }

    #[instrument(level = "info", skip(self, handle))]
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

    #[instrument(level = "info", skip(self, handle, size))]
    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> vfs::VfsResult<()> {
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

    fn write(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &[u8],
    ) -> vfs::VfsResult<usize> {
        let span = span!(Level::INFO, "write");
        let _guard = span.enter();

        // Get or create file state
        let file_state = {
            let mut files = self.files.lock();
            files
                .entry(handle.path.clone())
                .or_insert_with(FileState::new)
                .clone()
        };
        let is_batch_write = file_state.batch_open.load(Ordering::Acquire);
        log::debug!(
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
            span.record("pending_writes", pending_writes.len());
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

            log::debug!(
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

    #[instrument(level = "info", skip(self, data))]
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
                log::debug!("read page not found, returning empty data");
                return Ok::<usize, i32>(0);
            }

            let page = page_data.unwrap();
            let offset_in_page = offset % PAGE_SIZE;

            // Check if offset is beyond page size
            if offset_in_page >= page.len() {
                log::debug!("read offset is beyond page size");
                return Ok(0);
            }

            // Read as much data as available from this page, up to the requested length
            let end_offset_in_page = std::cmp::min(offset_in_page + data.len(), page.len());
            let d = page[offset_in_page..end_offset_in_page].to_vec();

            log::debug!("read data length: {} from page {}", data.len(), page_offset);

            let len = data.len().min(d.len());
            data[..len].copy_from_slice(&d[..len]);
            Ok(len)
        })
    }

    #[instrument(level = "info", skip(self))]
    fn close(&self, handle: Self::Handle) -> vfs::VfsResult<()> {
        log::debug!("close: path={} handle_id={}", handle.path, handle.handle_id);

        // Remove handle from lock manager
        self.lock_manager.remove_handle(&handle.path, handle.handle_id);

        // Clean up file state if needed (keep for batch writes)
        // Note: We keep file states around for batch operations, lock manager handles its own cleanup

        // Flush traces on every close to ensure data is written
        let guard = self._guard.lock();
        if let Some(guard) = &*guard {
            guard.flush();
        }

        Ok(())
    }

    fn device_characteristics(&self) -> i32 {
        log::debug!("device_characteristics");
        let mut characteristics: i32 = vfs::DEFAULT_DEVICE_CHARACTERISTICS;
        if self.capabilities.atomic_batch {
            characteristics |= sqlite_plugin::vars::SQLITE_IOCAP_BATCH_ATOMIC;
        }
        // TODO: Do we bother with SQLITE_IOCAP_IMMUTABLE if we're opened in read only mode?
        characteristics
    }

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: vfs::Pragma<'_>,
    ) -> Result<Option<String>, vfs::PragmaErr> {
        log::debug!("pragma: file2={:?}, pragma={:?}", handle.path, pragma);
        if pragma.name == "is_memory_server" {
            return Ok(Some("maybe?".to_string()));
        }
        Ok(None)
    }

    #[instrument(level = "info", skip(self, handle, op, _p_arg))]
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
        log::debug!("file_control: file={:?}, op={op_name}", handle.path);
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
                        log::debug!("write batch is empty, nothing to commit");
                        return Ok(());
                    }
                    let mut page_writes: HashMap<usize, Vec<_>> = HashMap::new();
                    for write in batch.iter() {
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
                            log::error!("error getting page during atomic write: {e}");
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

                            log::debug!(
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
        log::debug!("sector_size");
        self.capabilities.sector_size
    }

    #[instrument(level = "info", skip(self))]
    fn unlock(&self, handle: &mut Self::Handle, level: flags::LockLevel) -> vfs::VfsResult<()> {
        self.lock_manager.unlock(&handle.path, handle.handle_id, level)
    }
    #[instrument(level = "info", skip(self))]
    fn lock(&self, handle: &mut Self::Handle, level: flags::LockLevel) -> vfs::VfsResult<()> {
        self.lock_manager.lock(&handle.path, handle.handle_id, level)
    }
    #[instrument(level = "info", skip(self))]
    fn sync(&self, handle: &mut Self::Handle) -> vfs::VfsResult<()> {
        log::debug!("sync: path={}", handle.path);
        // self.runtime.block_on(async {
        //     let db = self.db.clone();
        //     db.flush().await.map_err(|e| {
        //         log::error!("error flushing database: {e}");
        //         sqlite_plugin::vars::SQLITE_IOERR_FSYNC
        //     })
        // })?;
        Ok(())
    }
}

pub const VFS_NAME: &CStr = c"grpsqlite";

static GRPC_VFS_INSTANCE: OnceLock<Arc<GrpcVfs>> = OnceLock::new();

pub fn set_vfs_context(rt: Handle, db: Arc<Db>) {
    let _ = GRPC_VFS_INSTANCE.set(Arc::new(GrpcVfs::new(rt, db)));
}

#[allow(clippy::expect_used)]
fn get_grpc_vfs() -> Arc<GrpcVfs> {
    GRPC_VFS_INSTANCE.get()
        .expect("GRPC_VFS_INSTANCE is not initialized")
        .clone()
}

#[allow(clippy::expect_used)]
fn setup_tracing() -> tracing_chrome::FlushGuard {
    use std::fs::File;
    use std::io::BufWriter;

    let (chrome_layer, guard) = ChromeLayerBuilder::new()
        .writer(BufWriter::new(
            File::create("s3qlite_trace.cpuprofile").expect("Failed to create profile file"),
        ))
        .build();

    // Don't call init() which would take over global logging
    // Instead, just set up tracing without interfering with existing log setup
    let subscriber = Registry::default().with(chrome_layer);

    // Only set the global default if there isn't one already
    let _ = tracing::subscriber::set_global_default(subscriber);

    guard
}

/// This function initializes the VFS statically.
/// Called automatically when the library is loaded.
///
/// # Safety
/// This function is safe to call from C as it only registers a VFS implementation
/// with SQLite and doesn't access any raw pointers or perform unsafe operations.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn initialize_grpsqlite() -> i32 {
    let vfs = get_grpc_vfs();

    if let Err(err) = vfs::register_static(
        VFS_NAME.to_owned(),
        (*vfs).clone(),
        vfs::RegisterOpts { make_default: true },
    ) {
        log::error!("Failed to initialize grpsqlite: {err}");
        return err;
    }

    // set the log level to trace
    log::set_max_level(log::LevelFilter::Trace);
    sqlite_plugin::vars::SQLITE_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn flush_traces() {
    let vfs = get_grpc_vfs();
    let guard = vfs._guard.lock().take();
    if let Some(guard) = guard {
        guard.flush();
        drop(guard);
    }
}

/// This function is called by `SQLite` when the extension is loaded. It registers
/// the memvfs VFS with `SQLite`.
///
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
/// The provided pointers must be valid `SQLite` API structures.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_grpsqlite_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_char,
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
) -> std::os::raw::c_int {
    let vfs = get_grpc_vfs();
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

    // set the log level to trace
    log::set_max_level(log::LevelFilter::Trace);

    sqlite_plugin::vars::SQLITE_OK_LOAD_PERMANENTLY
}
