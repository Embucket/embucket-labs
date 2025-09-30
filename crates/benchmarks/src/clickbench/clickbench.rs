use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use crate::util::{query_context, BenchmarkRun, CommonOpt};
use datafusion::error::{DataFusionError, Result};
use datafusion::common::exec_datafusion_err;
use datafusion::common::instant::Instant;
use structopt::StructOpt;
use core_executor::service::{make_test_execution_svc, ExecutionService};
use core_executor::session::UserSession;

/// Run the clickbench benchmark
///
/// The ClickBench[1] benchmarks are widely cited in the industry and
/// focus on grouping / aggregation / filtering. This runner uses the
/// scripts and queries from [2].
///
/// [1]: https://github.com/ClickHouse/ClickBench
/// [2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 0 and 42). If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to hits.parquet (single file) or `hits_partitioned`
    /// (partitioned, 100 files)
    #[structopt(
        parse(from_os_str),
        short = "p",
        long = "path",
        default_value = "crates/benchmarks/data/hits.parquet"
    )]
    path: PathBuf,

    /// Path to queries.sql (single file)
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "crates/benchmarks/queries/clickbench/queries.sql"
    )]
    queries_path: PathBuf,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

struct AllQueries {
    queries: Vec<String>,
}

impl AllQueries {
    fn try_new(path: &Path) -> Result<Self> {
        // ClickBench has all queries in a single file identified by line number
        let all_queries = std::fs::read_to_string(path)
            .map_err(|e| exec_datafusion_err!("Could not open {path:?}: {e}"))?;
        Ok(Self {
            queries: all_queries.lines().map(std::string::ToString::to_string).collect(),
        })
    }

    /// Returns the text of query `query_id`
    fn get_query(&self, query_id: usize) -> Result<&str> {
        self.queries
            .get(query_id)
            .ok_or_else(|| {
                let min_id = self.min_query_id();
                let max_id = self.max_query_id();
                exec_datafusion_err!(
                    "Invalid query id {query_id}. Must be between {min_id} and {max_id}"
                )
            })
            .map(String::as_str)
    }

    const fn min_query_id(&self) -> usize {
        0
    }

    const fn max_query_id(&self) -> usize {
        self.queries.len() - 1
    }
}
impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let queries = AllQueries::try_new(self.queries_path.as_path())?;
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => queries.min_query_id()..=queries.max_query_id(),
        };

        let service = make_test_execution_svc().await;
        let session = service.create_session("session_id").await?;
        {

            let state = session.ctx.state_ref();
            let mut write = state.write();
            let options = write.config_mut().options_mut();
            // The hits_partitioned dataset specifies string columns
            // as binary due to how it was written. Force it to strings
            options.execution.parquet.binary_as_string = true;
        }
        self.register_hits(&session).await?;

        let iterations = self.common.iterations;
        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            let mut millis = Vec::with_capacity(iterations);
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let sql = queries.get_query(query_id)?;
            println!("Q{query_id}: {sql}");

            for i in 0..iterations {
                let start = Instant::now();
                let mut user_query = session.query(sql, query_context());
                let results = user_query.execute().await?.records;
                let elapsed = start.elapsed();
                let ms = elapsed.as_secs_f64() * 1000.0;
                millis.push(ms);
                let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
                println!(
                    "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                );
                benchmark_run.write_iter(elapsed, row_count);
            }
            let avg = millis.iter().sum::<f64>() / millis.len() as f64;
            println!("Query {query_id} avg time: {avg:.2} ms");
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    /// Registers the `hits.parquet` as a table named `hits`
    async fn register_hits(&self, session: &Arc<UserSession>) -> Result<()> {
        let options = Default::default();
        let path = self.path.as_os_str().to_str().unwrap();
        session.ctx.register_parquet("hits", path, options)
            .await
            .map_err(|e| {
                DataFusionError::Context(
                    format!("Registering 'hits' as {path}"),
                    Box::new(e),
                )
            })
    }
}
