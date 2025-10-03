use std::ops::RangeInclusive;
use std::sync::Arc;

use super::{RunOpt, TPCH_TABLES, get_query_sql, get_tbl_tpch_table_schema, get_tpch_table_schema};
use crate::util::BenchmarkRun;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::{MemTable, TableProvider};
use datafusion::common::instant::Instant;
use datafusion::common::{DEFAULT_CSV_EXTENSION, DEFAULT_PARQUET_EXTENSION};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::logical_expr::col;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use log::info;

impl RunOpt {
    #[allow(clippy::print_stdout)]
    pub async fn run_df(self, query_range: RangeInclusive<usize>) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_df_query(query_id).await?;
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
    async fn benchmark_df_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let mut config = self
            .common
            .config()
            .with_collect_statistics(!self.disable_statistics);
        config.options_mut().optimizer.prefer_hash_join = self.common.prefer_hash_join;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        // register tables
        self.register_tables(&ctx).await?;

        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let sql = &get_query_sql(query_id)?;
            let mut result = vec![];
            for query in sql {
                result = self.execute_query(&ctx, query).await?;
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

    #[allow(clippy::print_stdout)]
    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in TPCH_TABLES {
            let table_provider = self.get_table(ctx, table).await?;

            if self.common.mem_table {
                println!("Loading table '{table}' into memory");
                let start = Instant::now();
                let memtable =
                    MemTable::load(table_provider, Some(self.partitions()), &ctx.state()).await?;
                println!(
                    "Loaded table '{}' into memory in {} ms",
                    table,
                    start.elapsed().as_millis()
                );
                ctx.register_table(*table, Arc::new(memtable))?;
            } else {
                ctx.register_table(*table, table_provider)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::print_stdout)]
    async fn execute_query(&self, ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let debug = self.common.debug;
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        if debug {
            println!("=== Logical plan ===\n{plan}\n");
        }

        let plan = state.optimize(&plan)?;
        if debug {
            println!("=== Optimized logical plan ===\n{plan}\n");
        }
        let physical_plan = state.create_physical_plan(&plan).await?;
        if debug {
            println!(
                "=== Physical plan ===\n{}\n",
                displayable(physical_plan.as_ref()).indent(true)
            );
        }
        let result = collect(physical_plan.clone(), state.task_ctx()).await?;
        if debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent(true)
            );
            if !result.is_empty() {
                // do not call print_batches if there are no batches as the result is confusing
                // and makes it look like there is a batch with no columns
                pretty::print_batches(&result)?;
            }
        }

        Ok(result)
    }

    #[allow(clippy::unwrap_used)]
    async fn get_table(&self, ctx: &SessionContext, table: &str) -> Result<Arc<dyn TableProvider>> {
        let path = self.path.to_str().unwrap();
        let table_format = self.file_format.as_str();
        let target_partitions = self.partitions();

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let (format, path, extension): (Arc<dyn FileFormat>, String, &'static str) =
            match table_format {
                // dbgen creates .tbl ('|' delimited) files without header
                "tbl" => {
                    let path = format!("{path}/{table}.tbl");

                    let format = CsvFormat::default()
                        .with_delimiter(b'|')
                        .with_has_header(false);

                    (Arc::new(format), path, ".tbl")
                }
                "csv" => {
                    let path = format!("{path}/{table}");
                    let format = CsvFormat::default()
                        .with_delimiter(b',')
                        .with_has_header(true);

                    (Arc::new(format), path, DEFAULT_CSV_EXTENSION)
                }
                "parquet" => {
                    let path = format!("{path}/{table}");
                    let format = ParquetFormat::default()
                        .with_options(ctx.state().table_options().parquet.clone());

                    (Arc::new(format), path, DEFAULT_PARQUET_EXTENSION)
                }
                other => {
                    unimplemented!("Invalid file format '{}'", other);
                }
            };

        let table_path = ListingTableUrl::parse(path)?;
        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_target_partitions(target_partitions)
            .with_collect_stat(state.config().collect_statistics());
        let schema = match table_format {
            "parquet" => options.infer_schema(&state, &table_path).await?,
            "tbl" => Arc::new(get_tbl_tpch_table_schema(table)),
            "csv" => Arc::new(get_tpch_table_schema(table)),
            _ => unreachable!(),
        };
        let options = if self.sorted {
            let key_column_name = schema.fields()[0].name();
            options.with_file_sort_order(vec![vec![col(key_column_name).sort(true, false)]])
        } else {
            options
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(schema);

        Ok(Arc::new(ListingTable::try_new(config)?))
    }
}

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}
