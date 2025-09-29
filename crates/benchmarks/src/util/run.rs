use datafusion::common::utils::get_available_parallelism;
use datafusion::{DATAFUSION_VERSION, error::Result};
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::Path,
    time::{Duration, SystemTime},
};

#[allow(clippy::expect_used)]
fn serialize_start_time<S>(start_time: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_u64(
        start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("current time is later than UNIX_EPOCH")
            .as_secs(),
    )
}
fn serialize_elapsed<S>(elapsed: &Duration, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ms = elapsed.as_secs_f64() * 1000.0;
    ser.serialize_f64(ms)
}
#[derive(Debug, Serialize)]
pub struct RunContext {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// `DataFusion` crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Start time
    #[serde(serialize_with = "serialize_start_time")]
    pub start_time: SystemTime,
    /// CLI arguments
    pub arguments: Vec<String>,
}

impl Default for RunContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RunContext {
    pub fn new() -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: get_available_parallelism(),
            start_time: SystemTime::now(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
        }
    }
}

/// A single iteration of a benchmark query
#[derive(Debug, Serialize)]
struct QueryIter {
    #[serde(serialize_with = "serialize_elapsed")]
    elapsed: Duration,
    row_count: usize,
}
/// A single benchmark case
#[derive(Debug, Serialize)]
pub struct BenchQuery {
    query: String,
    iterations: Vec<QueryIter>,
    #[serde(serialize_with = "serialize_start_time")]
    start_time: SystemTime,
}

/// collects benchmark run data and then serializes it at the end
pub struct BenchmarkRun {
    context: RunContext,
    queries: Vec<BenchQuery>,
    current_case: Option<usize>,
}

impl Default for BenchmarkRun {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkRun {
    // create new
    #[must_use]
    pub fn new() -> Self {
        Self {
            context: RunContext::new(),
            queries: vec![],
            current_case: None,
        }
    }
    pub fn start_new_case(&mut self, id: &str) {
        self.queries.push(BenchQuery {
            query: id.to_owned(),
            iterations: vec![],
            start_time: SystemTime::now(),
        });
        if let Some(c) = self.current_case.as_mut() {
            *c += 1;
        } else {
            self.current_case = Some(0);
        }
    }
    /// Write a new iteration to the current case
    pub fn write_iter(&mut self, elapsed: Duration, row_count: usize) {
        if let Some(idx) = self.current_case {
            self.queries[idx]
                .iterations
                .push(QueryIter { elapsed, row_count });
        } else {
            panic!("no cases existed yet");
        }
    }

    /// Stringify data into formatted json
    #[allow(clippy::unwrap_used)]
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut output = HashMap::<&str, Value>::new();
        output.insert("context", serde_json::to_value(&self.context).unwrap());
        output.insert("queries", serde_json::to_value(&self.queries).unwrap());
        serde_json::to_string_pretty(&output).unwrap()
    }

    /// Write data as json into output path if it exists.
    pub fn maybe_write_json(&self, maybe_path: Option<impl AsRef<Path>>) -> Result<()> {
        if let Some(path) = maybe_path {
            std::fs::write(path, self.to_json())?;
        }
        Ok(())
    }
}
