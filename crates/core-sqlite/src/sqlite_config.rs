#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub grpc_vfs_connect_timeout_secs: u64,
    pub local_cache_dir: Option<String>,
    pub max_cache_bytes: Option<u64>,
    /// Locally read values instead of going to the server. Risks stale data.
    pub local_reads: bool,
    /// Preload the cache on startup. Does not block reads. Will start from the DB head and download up to the max cache size.
    pub preload_cache: bool,
    pub preload_cache_concurrency: u32,
}

impl SqliteConfig {
    pub fn new() -> Self {
        Self {
            grpc_vfs_connect_timeout_secs: std::env::var("GRPC_VFS_CONNECT_TIMEOUT_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse::<u64>()
                .unwrap_or(10),
            local_cache_dir: std::env::var("LOCAL_CACHE_DIR").ok(),
            max_cache_bytes: std::env::var("MAX_CACHE_BYTES")
                .ok()
                .and_then(|s| s.parse::<u64>().ok()),
            local_reads: std::env::var("LOCAL_READS")
                .ok()
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or(false),
            preload_cache: std::env::var("PRELOAD_CACHE")
                .ok()
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or(false),
            preload_cache_concurrency: std::env::var("PRELOAD_CACHE_CONCURRENCY")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(4),
        }
    }
}
