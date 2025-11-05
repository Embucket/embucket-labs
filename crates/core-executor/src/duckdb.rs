use super::error::{self as ex_error};
use async_stream::stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::{
    DuckDBParameter, DuckDbConnection,
};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::sql::sql_provider_datafusion::to_execution_error;
use duckdb::ToSql;
use futures::{StreamExt, TryStreamExt};
use snafu::{OptionExt, ResultExt};
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;

pub async fn execute_duck_db_explain(
    duckdb_pool: Arc<DuckDbConnectionPool>,
    sql: &str,
) -> crate::Result<Vec<RecordBatch>> {
    let conn = duckdb_pool
        .connect()
        .await
        .map_err(to_execution_error)
        .context(crate::error::DataFusionSnafu)?;
    let duckdb_conn = conn
        .as_any()
        .downcast_ref::<DuckDbConnection>()
        .context(ex_error::DuckDbConnectionDowncastSnafu)?;
    // https://duckdb.org/docs/stable/guides/meta/explain
    let explain_sql = format!("EXPLAIN (format html) {sql}");
    let stream = query_duck_db_arrow(duckdb_conn, &explain_sql, &[])?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .context(crate::error::DataFusionSnafu)
}

fn query_duck_db_arrow(
    duckdb_conn: &DuckDbConnection,
    sql: &str,
    params: &[DuckDBParameter],
) -> crate::Result<SendableRecordBatchStream> {
    // Clone connection for blocking thread
    let conn = duckdb_conn
        .conn
        .try_clone()
        .context(crate::error::DuckdbSnafu)?;

    conn.execute("PRAGMA explain_output = 'all'", [])
        .context(crate::error::DuckdbSnafu)?;

    // Prepare statement and get schema
    let mut stmt = conn.prepare(sql).context(crate::error::DuckdbSnafu)?;
    let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(crate::error::DuckdbSnafu)?;
    let schema = result.get_schema();

    // Create async channel for record batches
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

    let sql = sql.to_string();
    let params = params.to_vec();
    let cloned_schema = schema.clone();

    let join_handle = tokio::task::spawn_blocking(move || -> crate::Result<()> {
        let mut stmt = conn.prepare(&sql).context(crate::error::DuckdbSnafu)?;
        let params: &[&dyn ToSql] = &params
            .iter()
            .map(|f| f.as_input_parameter())
            .collect::<Vec<_>>();
        let result: duckdb::ArrowStream<'_> = stmt
            .stream_arrow(params, cloned_schema)
            .context(crate::error::DuckdbSnafu)?;
        for batch in result {
            blocking_channel_send(&batch_tx, batch)?;
        }
        Ok(())
    });

    let stream = ReceiverStream::new(batch_rx)
        .map(Ok)
        .chain(stream! {
        match join_handle.await {
                Ok(Err(e)) => yield Err(DataFusionError::Execution(format!("DuckDB query failed: {e}"))),
                Err(join_err) => yield Err(DataFusionError::Execution(format!("DuckDB thread join failed: {join_err}"))),
                _ => {}
                }
        });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(stream),
    )))
}

fn blocking_channel_send<T>(channel: &Sender<T>, item: T) -> crate::Result<()> {
    channel
        .blocking_send(item)
        .map_err(|e| duckdbconn::Error::ChannelError {
            message: e.to_string(),
        })
        .context(crate::error::DuckdbConnectionSnafu)
}

pub fn is_select_statement(stmt: &DFStatement) -> bool {
    matches!(stmt, DFStatement::Statement(inner) if matches!(**inner, Statement::Query(_)))
}
