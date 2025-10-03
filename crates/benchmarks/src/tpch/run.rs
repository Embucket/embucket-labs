use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;

use super::{TPCH_TABLES, get_query_sql, get_tpch_table_sql};
use crate::util::{
    BenchmarkRun, CommonOpt, create_catalog, make_test_execution_svc, query_context,
    set_session_variable_bool, set_session_variable_number,
};

use core_executor::service::{CoreExecutionService, ExecutionService};
use core_executor::session::UserSession;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::instant::Instant;
use datafusion::common::utils::get_available_parallelism;
use datafusion::error::Result;
use log::info;
use structopt::StructOpt;

/// Run the tpch benchmark.
///
/// This benchmarks is derived from the [TPC-H][1] version
/// [2.17.1]. The data and answers are generated using `tpch-gen` from
/// [2].
///
/// [1]: http://www.tpc.org/tpch/
/// [2]: https://github.com/databricks/tpch-dbgen.git,
/// [2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,
    /// Common options
    #[structopt(flatten)]
    pub common: CommonOpt,
    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    pub path: PathBuf,

    /// File format: `csv` or `parquet`
    #[structopt(short = "f", long = "format", default_value = "parquet")]
    pub file_format: String,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    pub output_path: Option<PathBuf>,

    /// Whether to disable collection of statistics (and cost based optimizations) or not.
    #[structopt(short = "S", long = "disable-statistics")]
    pub disable_statistics: bool,

    /// Mark the first column of each table as sorted in ascending order.
    /// The tables should have been created with the `--sort` option for this to have any effect.
    #[structopt(short = "t", long = "sorted")]
    pub sorted: bool,
}

const TPCH_QUERY_START_ID: usize = 1;
const TPCH_QUERY_END_ID: usize = 22;

impl RunOpt {
    #[allow(clippy::print_stdout)]
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => TPCH_QUERY_START_ID..=TPCH_QUERY_END_ID,
        };
        if self.common.datafusion {
            self.run_df(query_range).await
        } else {
            self.run_embucket(query_range).await
        }
    }

    #[allow(clippy::print_stdout, clippy::unwrap_used)]
    pub async fn run_embucket(self, query_range: RangeInclusive<usize>) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();

        println!("Create service, volume, database, schema");
        let service = make_test_execution_svc().await;
        let session = service.create_session("session_id").await?;
        let path = self.path.to_str().unwrap();
        create_catalog(path, &session, self.common.mem_table).await?;

        // Set the number of output parquet files during copy into
        set_session_variable_number(
            "execution.minimum_parallel_output_files",
            self.common.output_files_number,
            &session,
        )
        .await?;
        // Turn on Parquet filter pushdown if requested
        if self.common.pushdown {
            set_session_variable_bool("execution.parquet.pushdown_filters ", true, &session)
                .await?;
            set_session_variable_bool("execution.parquet.reorder_filters", true, &session).await?;
        }
        self.create_tables(&session).await?;

        // Run queries
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_embucket_query(query_id, &service).await?;
            for iter in query_run {
                benchmark_run.write_iter(iter.elapsed, iter.row_count);
            }
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    #[allow(
        clippy::cast_precision_loss,
        clippy::as_conversions,
        clippy::print_stdout,
        clippy::unwrap_used
    )]
    async fn benchmark_embucket_query(
        &self,
        query_id: usize,
        service: &Arc<CoreExecutionService>,
    ) -> Result<Vec<QueryResult>> {
        let session_id = format!("session_id_{query_id}");
        let session = service.create_session(&session_id).await?;

        // Set prefer_hash_join session variable
        set_session_variable_bool(
            "optimizer.prefer_hash_join",
            self.common.prefer_hash_join,
            &session,
        )
        .await?;

        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let sql = &get_query_sql(query_id)?;
            let mut result = vec![];
            for query in sql {
                let mut user_query = session.query(query, query_context());
                result = user_query.execute().await?.records;
            }

            let elapsed = start.elapsed(); //.as_secs_f64() * 1000.0;
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);
            info!("output:\n\n{}\n\n", pretty_format_batches(&result)?);
            let row_count = result.iter().map(RecordBatch::num_rows).sum();
            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");

        Ok(query_results)
    }

    #[allow(clippy::unwrap_used)]
    async fn create_tables(&self, session: &Arc<UserSession>) -> Result<()> {
        let path = self.path.to_str().unwrap();
        for table in TPCH_TABLES {
            let table_sql = get_tpch_table_sql(table).unwrap();
            let mut table_query = session.query(table_sql, query_context());
            table_query.execute().await?;
            let data_sql = format!(
                "COPY INTO {table}
                FROM 'file://{path}/{table}' FILE_FORMAT = ( TYPE = PARQUET );"
            );
            let mut data_query = session.query(data_sql, query_context());
            data_query.execute().await?;
        }
        Ok(())
    }

    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.common.iterations
    }

    #[must_use]
    pub fn partitions(&self) -> usize {
        self.common
            .partitions
            .unwrap_or_else(get_available_parallelism)
    }
}

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}
