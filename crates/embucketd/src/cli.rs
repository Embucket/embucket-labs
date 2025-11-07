use clap::{Parser, ValueEnum};
use core_executor::utils::{DEFAULT_QUERY_HISTORY_ROWS_LIMIT, MemPoolType};
use std::path::PathBuf;
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
        long,
        env = "METASTORE_CONFIG",
        value_name = "PATH",
        help = "Path to YAML config describing volumes/databases to seed the metastore"
    )]
    pub metastore_config: Option<PathBuf>,

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
