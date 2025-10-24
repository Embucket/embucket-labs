use crate::error::{self as ex_error, Result};
use arrow_schema::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::sql::parser::Statement as DFStatement;
use duckdb::Connection;
use snafu::ResultExt;
use sqlparser::ast::Statement;
use std::sync::Arc;

pub fn execute_duck_db_explain(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>> {
    let explain_sql = format!("EXPLAIN (format html) {sql}");
    conn.execute("PRAGMA explain_output = 'all'", [])
        .context(ex_error::DuckdbSnafu)?;
    let (res, _) = query_duck_db_arrow(conn, &explain_sql)?;
    Ok(res)
}

pub fn query_duck_db_arrow(
    duckdb_conn: &Connection,
    sql: &str,
) -> Result<(Vec<RecordBatch>, SchemaRef)> {
    // Clone connection for blocking thread
    let sql = sql.to_string();

    // Prepare statement and get schema
    let mut stmt = duckdb_conn
        .prepare(&sql)
        .context(crate::error::DuckdbSnafu)?;
    let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(crate::error::DuckdbSnafu)?;
    let schema = result.get_schema();
    let res = result.collect();
    Ok((res, schema))
}

#[must_use]
pub fn is_select_statement(stmt: &DFStatement) -> bool {
    matches!(stmt, DFStatement::Statement(inner) if matches!(**inner, Statement::Query(_)))
}

pub fn apply_connection_setup_queries(conn: &Connection, setup_queries: &[Arc<str>]) -> Result<()> {
    for query in setup_queries {
        conn.execute(query, []).context(ex_error::DuckdbSnafu)?;
    }
    Ok(())
}
