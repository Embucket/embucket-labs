use crate::error::{self as ex_error, Result};
use async_stream::stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use duckdb::{ffi, Connection, Error};
use futures::{StreamExt, TryStreamExt};
use snafu::ResultExt;
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;


pub async fn execute_duck_db_explain(
    conn: &Connection,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let sql = sql.to_string();
    let conn = conn.try_clone().context(ex_error::DuckdbSnafu)?;

    tokio::task::spawn_blocking(move || {
        let explain_sql = format!("EXPLAIN (format html) {sql}");
        conn.execute("PRAGMA explain_output = 'all'", [])
            .context(ex_error::DuckdbSnafu)?;
        let mut stmt = conn.prepare(&explain_sql).context(ex_error::DuckdbSnafu)?;
        let arrow = stmt.query_arrow([]).context(ex_error::DuckdbSnafu)?;
        Ok(arrow.collect())
    }).await.unwrap()
}

pub fn query_duck_db_arrow(
    duckdb_conn: &Connection,
    sql: &str,
) -> Result<SendableRecordBatchStream> {
    // Clone connection for blocking thread
    let conn = duckdb_conn.try_clone().context(crate::error::DuckdbSnafu)?;
    let sql = sql.to_string();

    // Prepare statement and get schema
    let mut stmt = duckdb_conn.prepare(&sql).context(crate::error::DuckdbSnafu)?;
    let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(crate::error::DuckdbSnafu)?;
    let schema = result.get_schema();

    // Create async channel for record batches
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);
    let cloned_schema = schema.clone();

    let join_handle = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut stmt = conn.prepare(&sql).context(crate::error::DuckdbSnafu)?;
        let result: duckdb::ArrowStream<'_> = stmt
            .stream_arrow([], cloned_schema)
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

fn blocking_channel_send<T>(channel: &Sender<T>, item: T) -> Result<()> {
    channel
        .blocking_send(item)
        .map_err(|e| {
            DataFusionError::Execution(e.to_string())
        })
        .context(ex_error::DataFusionSnafu)
}
pub fn is_select_statement(stmt: &DFStatement) -> bool {
    matches!(stmt, DFStatement::Statement(inner) if matches!(**inner, Statement::Query(_)))
}

pub fn apply_connection_setup_queries(
    conn: &Connection,
    setup_queries: Vec<Arc<str>>,
) -> Result<()> {
    for query in &setup_queries {
        conn.execute(query, []).context(ex_error::DuckdbSnafu)?;
    }
    Ok(())
}

fn get_arrow_data(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>> {
    Ok(conn
        .prepare(sql)
        .context(ex_error::DuckdbSnafu)?
        .query_arrow([])
        .context(ex_error::DuckdbSnafu)?
        .collect())
}
