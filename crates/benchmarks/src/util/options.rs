use datafusion::common::utils::get_available_parallelism;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::DiskManager;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use std::num::NonZeroUsize;
use std::sync::Arc;
use structopt::StructOpt;

// hack to avoid `default_value is meaningless for bool` errors
pub type BoolDefaultTrue = bool;

const NUM_TRACKED_CONSUMERS: usize = 5;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, StructOpt, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    pub iterations: usize,

    /// The number of output parquet files
    #[structopt(long = "output_files_number", default_value = "1")]
    pub output_files_number: usize,

    /// If true then hash join used, if false then sort merge join
    /// True by default.
    #[structopt(short = "j", long = "prefer_hash_join", default_value = "true")]
    pub prefer_hash_join: BoolDefaultTrue,

    #[structopt(short = "df", long = "datafusion")]
    pub datafusion: bool,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    #[structopt(short = "n", long = "partitions")]
    pub partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    pub batch_size: usize,

    /// The memory pool type to use, should be one of "fair" or "greedy"
    #[structopt(long = "mem-pool-type", default_value = "fair")]
    pub mem_pool_type: String,

    /// Memory limit (e.g. '100M', '1.5G'). If not specified, run all pre-defined memory limits for given query
    /// if there's any, otherwise run with no memory limit.
    #[structopt(long = "memory-limit", parse(try_from_str = parse_memory_limit))]
    pub memory_limit: Option<usize>,

    /// The amount of memory to reserve for sort spill operations. `DataFusion`'s default value will be used
    /// if not specified.
    #[structopt(long = "sort-spill-reservation-bytes", parse(try_from_str = parse_memory_limit))]
    pub sort_spill_reservation_bytes: Option<usize>,

    /// Activate debug mode to see more details
    #[structopt(short, long)]
    pub debug: bool,

    /// Load the data into a memory before executing the query
    #[structopt(short = "m", long = "mem-table")]
    pub mem_table: bool,

    /// If specified, enables Parquet Filter Pushdown.
    ///
    /// Specifically, it enables:
    /// * `pushdown_filters = true`
    /// * `reorder_filters = true`
    #[structopt(long = "pushdown")]
    pub pushdown: bool,
}

impl CommonOpt {
    /// Return an appropriately configured `SessionConfig`
    pub fn config(&self) -> Result<SessionConfig> {
        SessionConfig::from_env().map(|config| self.update_config(config))
    }

    /// Modify the existing config appropriately
    #[must_use]
    pub fn update_config(&self, config: SessionConfig) -> SessionConfig {
        let mut config = config
            .with_target_partitions(self.partitions.unwrap_or(get_available_parallelism()))
            .with_batch_size(self.batch_size);
        if let Some(sort_spill_reservation_bytes) = self.sort_spill_reservation_bytes {
            config = config.with_sort_spill_reservation_bytes(sort_spill_reservation_bytes);
        }
        config
    }

    /// Return an appropriately configured `RuntimeEnvBuilder`
    #[allow(clippy::unwrap_used)]
    pub fn runtime_env_builder(&self) -> Result<RuntimeEnvBuilder> {
        let mut rt_builder = RuntimeEnvBuilder::new();
        if let Some(memory_limit) = self.memory_limit {
            let pool: Arc<dyn MemoryPool> = match self.mem_pool_type.as_str() {
                "fair" => Arc::new(TrackConsumersPool::new(
                    FairSpillPool::new(memory_limit),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                )),
                "greedy" => Arc::new(TrackConsumersPool::new(
                    GreedyMemoryPool::new(memory_limit),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                )),
                _ => {
                    return Err(DataFusionError::Configuration(format!(
                        "Invalid memory pool type: {}",
                        self.mem_pool_type
                    )));
                }
            };
            rt_builder = rt_builder
                .with_memory_pool(pool)
                .with_disk_manager_builder(DiskManager::builder());
        }
        Ok(rt_builder)
    }
}

/// Parse memory limit from string to number of bytes
/// e.g. '1.5G', '100M' -> 1572864
#[allow(
    clippy::cast_possible_truncation,
    clippy::as_conversions,
    clippy::cast_sign_loss
)]
fn parse_memory_limit(limit: &str) -> Result<usize, String> {
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("Failed to parse number from memory limit '{limit}'"))?;

    match unit {
        "K" => Ok((number * 1024.0) as usize),
        "M" => Ok((number * 1024.0 * 1024.0) as usize),
        "G" => Ok((number * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => Err(format!(
            "Unsupported unit '{unit}' in memory limit '{limit}'"
        )),
    }
}
