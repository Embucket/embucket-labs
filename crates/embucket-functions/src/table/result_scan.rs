use crate::table::errors;
use crate::utils::block_in_new_runtime;
use core_history::result_set::ResultSet;
use core_history::{GetQueriesParams, HistoryStore, QueryRecord};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::json::StructMode;
use datafusion::arrow::json::reader::ReaderBuilder;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue, exec_err};
use datafusion_expr::Expr;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ResultScanFunc {
    history_store: Arc<dyn HistoryStore>,
}

/// `RESULT_SCAN`
/// Returns the result set of a previous command as if the result was a table
///
/// Syntax: `RESULT_SCAN` ( { '<`query_id`>' | <`query_index`>  | `LAST_QUERY_ID()` } )
///
/// Arguments
/// `query_id` or `query_index` or `LAST_QUERY_ID()`
/// A specification of a query you executed within the last 24 hours in any session,
/// an integer index of a query in the current session, or the `LAST_QUERY_ID` function,
/// which returns the ID of a query within your current session.
/// Query indexes are relative to the first query in the current session (if positive)
/// or to the most recent query (if negative).
/// For example, RESULT_SCAN(-1) is equivalent to `RESULT_SCAN(LAST_QUERY_ID())`.
impl ResultScanFunc {
    #[must_use]
    pub fn new(history_store: Arc<dyn HistoryStore>) -> Self {
        Self { history_store }
    }

    pub fn last_query_id(&self, index: i64) -> DFResult<ScalarValue> {
        let history_store = self.history_store.clone();

        let id = block_in_new_runtime(async move {
            let queries = history_store
                .get_queries(GetQueriesParams::default())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_iter()
                .map(|q| q.id.to_string())
                .collect::<Vec<_>>();
            let query_id = get_query_by_index(&queries, index)
                .ok_or_else(|| errors::NoQueryFoundForIndexSnafu { index }.build())?;
            Ok::<String, DataFusionError>(query_id)
        })??;
        Ok(utf8_val(&id))
    }

    pub fn read_query_batches(&self, query_id: &str) -> DFResult<(SchemaRef, Vec<RecordBatch>)> {
        let history_store = self.history_store.clone();
        let query_id_parsed = query_id
            .parse::<i64>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let query_record = block_in_new_runtime(async move {
            let record = history_store
                .get_query(query_id_parsed.into())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok::<QueryRecord, DataFusionError>(record)
        })??;

        if query_record.error.is_some() {
            return exec_err!(
                "Query {query_id_parsed} has not been executed successfully: {:?}",
                query_record.error
            );
        }

        let result_json = query_record.result.ok_or_else(|| {
            errors::NoResultDataForQueryIdSnafu {
                query_id: query_id_parsed,
            }
            .build()
        })?;

        // Deserialize ResultSet string
        let result_set: ResultSet = serde_json::from_str(&result_json)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let arrow_json = convert_resultset_to_arrow_json_lines(&result_set)?;

        // Parse schema from serialized JSON
        let schema_value = serde_json::from_str(&result_set.schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema_ref: SchemaRef = Arc::new(schema_value);
        let json_reader = ReaderBuilder::new(schema_ref.clone())
            .with_struct_mode(StructMode::ListOnly)
            .build(Cursor::new(&arrow_json))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let batches = json_reader
            .collect::<arrow::error::Result<Vec<RecordBatch>>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok((schema_ref, batches))
    }
}

impl TableFunctionImpl for ResultScanFunc {
    fn call(&self, args: &[(Expr, Option<String>)]) -> DFResult<Arc<dyn TableProvider>> {
        let query_id = match args.first() {
            Some((Expr::Literal(ScalarValue::Utf8(Some(query_id)), _), _)) => utf8_val(query_id),
            Some((Expr::ScalarFunction(fun), _)) => {
                if fun.name().to_lowercase() == "last_query_id" {
                    let index = match fun.args.first() {
                        Some(Expr::Literal(value, _)) => value.clone().try_into().unwrap_or(-1),
                        _ => -1,
                    };
                    self.last_query_id(index)?
                } else {
                    return exec_err!(
                        "result_scan() expects a single string argument or last_query_id()"
                    );
                }
            }
            _ => return exec_err!("result_scan() expects a single integer argument"),
        };
        let (schema, record_batches) = self.read_query_batches(&query_id.to_string())?;
        Ok(Arc::new(MemTable::try_new(schema, vec![record_batches])?))
    }
}

impl PartialEq for ResultScanFunc {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.history_store, &other.history_store)
    }
}

impl Eq for ResultScanFunc {}

impl std::hash::Hash for ResultScanFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the data pointer of the trait object to get a stable identity per instance
        let trait_obj_ptr = Arc::as_ptr(&self.history_store);
        // SAFETY: converting fat pointer to (data, vtable) pair and hashing data pointer
        let (data_ptr, _vtable_ptr): (*const (), *const ()) =
            unsafe { std::mem::transmute(trait_obj_ptr) };
        state.write_usize(data_ptr as usize);
        state.write(b"ResultScanFunc");
    }
}

fn get_query_by_index(queries: &[String], index: i64) -> Option<String> {
    match index {
        i if i < 0 => {
            let abs = i.checked_abs()?.checked_sub(1)?;
            let abs_usize = usize::try_from(abs).ok()?;
            queries.get(abs_usize).cloned()
        }
        i if i > 0 => {
            let len = queries.len();
            let rev_index = usize::try_from(i).ok()?;
            queries.get(len.checked_sub(rev_index)?).cloned()
        }
        _ => None,
    }
}

fn utf8_val(val: impl Into<String>) -> ScalarValue {
    ScalarValue::Utf8(Some(val.into()))
}

fn convert_resultset_to_arrow_json_lines(
    result_set: &ResultSet,
) -> Result<String, DataFusionError> {
    let mut lines = String::new();
    for row in &result_set.rows {
        let json_value = serde_json::Value::Array(row.0.clone());
        lines.push_str(
            &serde_json::to_string(&json_value)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        lines.push('\n');
    }
    Ok(lines)
}
