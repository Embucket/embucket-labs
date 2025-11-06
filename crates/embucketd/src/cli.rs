use clap::{Parser, ValueEnum};
use core_executor::utils::DEFAULT_QUERY_HISTORY_ROWS_LIMIT;
use core_executor::utils::MemPoolType;
use object_store::{
    ObjectStore, Result as ObjectStoreResult, aws::AmazonS3Builder, aws::S3ConditionalPut,
    local::LocalFileSystem, memory::InMemory,
};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::filter::LevelFilter;

#[derive(Parser)]
#[command(version, about, long_about=None)]
pub struct CliOpts {
    #[arg(
        long,
        env = "NO_BOOTSTRAP",
        default_value = "false",
        help = "Disable bootstrap functionality"
    )]
    pub no_bootstrap: bool,

    #[arg(
        short,
        long,
        value_enum,
        env = "OBJECT_STORE_BACKEND",
        default_value = "memory",
        help = "Backend to use for state storage"
    )]
    backend: StoreBackend,

    #[arg(
        long,
        env = "AWS_ACCESS_KEY_ID",
        required_if_eq("backend", "s3"),
        help = "AWS Access Key ID",
        help_heading = "S3 Backend Options"
    )]
    access_key_id: Option<String>,
    #[arg(
        long,
        env = "AWS_SECRET_ACCESS_KEY",
        required_if_eq("backend", "s3"),
        help = "AWS Secret Access Key",
        help_heading = "S3 Backend Options"
    )]
    secret_access_key: Option<String>,
    #[arg(
        long,
        env = "AWS_REGION",
        required_if_eq("backend", "s3"),
        help = "AWS Region",
        help_heading = "S3 Backend Options"
    )]
    region: Option<String>,
    #[arg(
        long,
        env = "S3_BUCKET",
        required_if_eq("backend", "s3"),
        help = "S3 Bucket Name",
        help_heading = "S3 Backend Options"
    )]
    bucket: Option<String>,
    #[arg(
        long,
        env = "S3_ENDPOINT",
        help = "S3 Endpoint (Optional)",
        help_heading = "S3 Backend Options"
    )]
    endpoint: Option<String>,
    #[arg(
        long,
        env = "S3_ALLOW_HTTP",
        help = "Allow HTTP for S3 (Optional)",
        help_heading = "S3 Backend Options"
    )]
    allow_http: Option<bool>,

    #[arg(
        long,
        env = "FILE_STORAGE_PATH",
        required_if_eq("backend", "file"),
        help_heading = "File Backend Options",
        help = "Path to the directory where files will be stored"
    )]
    file_storage_path: Option<PathBuf>,

    #[arg(short, long, env = "SLATEDB_PREFIX", default_value = "state")]
    pub slatedb_prefix: String,

    #[arg(
        long,
        env = "BUCKET_HOST",
        default_value = "localhost",
        help = "Host to bind to"
    )]
    pub host: Option<String>,

    #[arg(
        long,
        env = "BUCKET_PORT",
        default_value = "3000",
        help = "Port to bind to"
    )]
    pub port: Option<u16>,

    #[arg(
        long,
        env = "WEB_ASSETS_PORT",
        default_value = "8080",
        help = "Port of web assets server to bind to"
    )]
    pub assets_port: Option<u16>,

    #[arg(
        long,
        env = "CATALOG_URL",
        default_value = "http://127.0.0.1:3000",
        help = "Iceberg catalog url"
    )]
    pub catalog_url: Option<String>,

    #[arg(
        long,
        env = "CORS_ENABLED",
        help = "Enable CORS",
        default_value = "true"
    )]
    pub cors_enabled: Option<bool>,

    #[arg(
        long,
        env = "CORS_ALLOW_ORIGIN",
        default_value = "http://localhost:8080",
        required_if_eq("cors_enabled", "true"),
        help = "CORS Allow Origin"
    )]
    pub cors_allow_origin: Option<String>,

    #[arg(
        short,
        long,
        default_value = "json",
        env = "DATA_FORMAT",
        help = "Data serialization format in Snowflake v1 API"
    )]
    pub data_format: Option<String>,

    #[arg(
        long,
        env = "SQL_PARSER_DIALECT",
        default_value = "snowflake",
        help = "SQL parser dialect, can be 'snowflake', 'postgres', 'mysql', 'generic', etc."
    )]
    pub sql_parser_dialect: Option<String>,

    #[arg(
        long,
        env = "MAX_CONCURRENCY_LEVEL",
        default_value = "8",
        help = "Maximum number of running queries at the same time"
    )]
    pub max_concurrency_level: usize,

    #[arg(
        long,
        env = "QUERY_TIMEOUT_SECS",
        default_value = "1200",
        help = "Maximum duration in seconds a single query is allowed to run"
    )]
    pub query_timeout_secs: u64,

    #[arg(
        long,
        value_enum,
        env = "MEM_POOL_TYPE",
        default_value = "greedy",
        help = "Memory pool type for query execution, can be 'greedy' or 'fair'"
    )]
    pub mem_pool_type: MemPoolType,

    #[arg(
        long,
        env = "MEM_POOL_SIZE_MB",
        help = "Maximum memory pool size in megabytes"
    )]
    pub mem_pool_size_mb: Option<usize>,

    #[arg(
        long,
        env = "MEM_ENABLE_TRACK_CONSUMERS_POOL",
        help = "Wrap memory pool with TrackConsumersPool for tracking per-consumer memory usage"
    )]
    pub mem_enable_track_consumers_pool: Option<bool>,

    #[arg(
        long,
        env = "DISK_POOL_SIZE_MB",
        help = "Maximum disk pool size in megabytes (for spilling)"
    )]
    pub disk_pool_size_mb: Option<usize>,

    #[arg(
        long,
        env = "ALLOC_TRACING",
        default_value = "true",
        help = "Enable memory tracing functionality"
    )]
    pub alloc_tracing: Option<bool>,

    #[arg(
        long,
        env = "QUERY_HISTORY_ROWS_LIMIT",
        default_value_t = DEFAULT_QUERY_HISTORY_ROWS_LIMIT,
        help = "Maximum number of rows in separate query record"
    )]
    pub query_history_rows_limit: usize,

    #[arg(
        long,
        env = "QUERY_HISTORY_DB_NAME",
        default_value = "sqlite_data/queries.db"
    )]
    pub query_history_db_name: String,

    #[arg(
        long,
        env = "QUERY_RESULTS_DB_NAME",
        default_value = "sqlite_data/results.db"
    )]
    pub query_results_db_name: String,

    // should unset JWT_SECRET env var after loading
    #[arg(
        long,
        env = "JWT_SECRET",
        hide_env_values = true,
        help = "JWT secret for auth"
    )]
    jwt_secret: Option<String>,

    #[arg(
        long,
        env = "ICEBERG_REST_BEARER_TOKEN",
        hide_env_values = true,
        help = "Bearer token for Iceberg REST API authorization"
    )]
    pub iceberg_rest_bearer_token: Option<String>,

    #[arg(
        long,
        env = "AUTH_DEMO_USER",
        value_parser = clap::builder::NonEmptyStringValueParser::new(),
        default_value = "embucket",
        help = "User for auth demo"
    )]
    pub auth_demo_user: Option<String>,

    #[arg(
        long,
        env = "AUTH_DEMO_PASSWORD",
        value_parser = clap::builder::NonEmptyStringValueParser::new(),
        default_value = "embucket",
        help = "Password for auth demo"
    )]
    pub auth_demo_password: Option<String>,

    #[arg(
        long,
        value_enum,
        env = "TRACING_LEVEL",
        default_value = "info",
        help = "Tracing level, it can be overrided by *RUST_LOG* env var"
    )]
    pub tracing_level: TracingLevel,

    #[arg(
        long,
        value_enum,
        env = "span_processor",
        default_value = "batch-span-processor",
        help = "Tracing span processor"
    )]
    pub tracing_span_processor: TracingSpanProcessor,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum StoreBackend {
    S3,
    File,
    Memory,
}

impl CliOpts {
    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn object_store_backend(&self) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
        match self.backend {
            StoreBackend::S3 => {
                let s3_allow_http = self.allow_http.unwrap_or(false);

                let s3_builder = AmazonS3Builder::new()
                    .with_access_key_id(self.access_key_id.clone().unwrap())
                    .with_secret_access_key(self.secret_access_key.clone().unwrap())
                    .with_region(self.region.clone().unwrap())
                    .with_bucket_name(self.bucket.clone().unwrap())
                    .with_conditional_put(S3ConditionalPut::ETagMatch);

                if let Some(endpoint) = &self.endpoint {
                    s3_builder
                        .with_endpoint(endpoint)
                        .with_allow_http(s3_allow_http)
                        .build()
                        .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                } else {
                    s3_builder
                        .build()
                        .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                }
            }
            StoreBackend::File => {
                let file_storage_path = self.file_storage_path.clone().unwrap();
                let path = file_storage_path.as_path();
                if !path.exists() || !path.is_dir() {
                    fs::create_dir(path).unwrap();
                }
                LocalFileSystem::new_with_prefix(file_storage_path)
                    .map(|fs| Arc::new(fs) as Arc<dyn ObjectStore>)
            }
            StoreBackend::Memory => Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>),
        }
    }

    #[cfg(feature = "ui")]
    // method resets a secret env
    pub fn jwt_secret(&self) -> String {
        unsafe {
            std::env::remove_var("JWT_SECRET");
        }
        self.jwt_secret.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum TracingLevel {
    Off,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum TracingSpanProcessor {
    BatchSpanProcessor,
    BatchSpanProcessorExperimentalAsyncRuntime,
}

#[allow(clippy::from_over_into)]
impl Into<LevelFilter> for TracingLevel {
    fn into(self) -> LevelFilter {
        match self {
            Self::Off => LevelFilter::OFF,
            Self::Info => LevelFilter::INFO,
            Self::Debug => LevelFilter::DEBUG,
            Self::Trace => LevelFilter::TRACE,
        }
    }
}

impl std::fmt::Display for TracingLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Off => write!(f, "off"),
            Self::Info => write!(f, "info"),
            Self::Debug => write!(f, "debug"),
            Self::Trace => write!(f, "trace"),
        }
    }
}
