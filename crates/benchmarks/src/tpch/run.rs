use std::path::PathBuf;
use std::sync::Arc;

use super::{TPCH_TABLES, get_query_sql, get_tpch_table_sql};
use crate::util::{BenchmarkRun, CommonOpt};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::instant::Instant;
use datafusion::error::Result;

use core_executor::models::QueryContext;
use core_executor::service::{ExecutionService, make_test_execution_svc};
use core_executor::session::UserSession;
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
    common: CommonOpt,
    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,
    /// Path to machine-readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

const TPCH_QUERY_START_ID: usize = 1;
const TPCH_QUERY_END_ID: usize = 22;

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => TPCH_QUERY_START_ID..=TPCH_QUERY_END_ID,
        };

        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(query_id).await?;
            for iter in query_run {
                benchmark_run.write_iter(iter.elapsed, iter.row_count);
            }
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let service = make_test_execution_svc().await;
        let session = service.create_session("session_id").await?;

        self.create_catalog(&session).await?;
        self.create_tables(&session).await?;

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
    async fn create_catalog(&self, session: &Arc<UserSession>) -> Result<()> {
        let path = self.path.to_str().unwrap();

        let volume_sql = format!(
            "CREATE EXTERNAL VOLUME IF NOT EXISTS test STORAGE_LOCATIONS = (\
        (NAME = 'file_vol' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{path}/data'))"
        );
        let mut volume_query = session.query(volume_sql, query_context());
        volume_query.execute().await?;

        let database_sql = "CREATE DATABASE IF NOT EXISTS bench EXTERNAL_VOLUME = test";
        let mut database_query = session.query(database_sql, query_context());
        database_query.execute().await?;

        let schema_sql = "CREATE SCHEMA IF NOT EXISTS bench.benchmark";
        let mut schema_query = session.query(schema_sql, query_context());
        schema_query.execute().await?;
        Ok(())
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
    const fn iterations(&self) -> usize {
        self.common.iterations
    }
}

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}

fn query_context() -> QueryContext {
    QueryContext::new(
        Some("bench".to_string()),
        Some("benchmark".to_string()),
        None,
    )
}
