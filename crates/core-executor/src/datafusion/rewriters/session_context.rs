use crate::models::QueryContext;
use crate::running_queries::{AbortQuery, RunningQueries};
use core_history::{GetQueriesParams, HistoryStore, QueryRecordId};
use datafusion::arrow::array::{ListArray, ListBuilder, StringBuilder};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use df_catalog::block_in_new_runtime;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

pub struct SessionContextExprRewriter {
    pub database: String,
    pub schema: String,
    pub schemas: Vec<String>,
    pub warehouse: String,
    pub session_id: String,
    pub version: String,
    pub query_context: QueryContext,
    pub history_store: Arc<dyn HistoryStore>,
    pub running_queries: Arc<dyn RunningQueries>,
}

impl SessionContextExprRewriter {
    fn rewrite_expr(&self, expr: Expr) -> Expr {
        let mut rewriter = ExprRewriter { rewriter: self };
        expr.clone()
            .rewrite(&mut rewriter)
            .map(|t| t.data)
            .unwrap_or(expr)
    }

    pub fn rewrite_plan(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let new_exprs = plan
            .expressions()
            .into_iter()
            .map(|e| self.rewrite_expr(e))
            .collect();
        let inputs = plan
            .inputs()
            .iter()
            .map(|p| (*p).clone())
            .collect::<Vec<_>>();
        plan.with_new_exprs(new_exprs, inputs)
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn last_query_id(&self, index: i64) -> Result<ScalarValue> {
        let history_store = self.history_store.clone();
        let worksheet_id = self.query_context.worksheet_id;

        let id = block_in_new_runtime(async move {
            let mut params = GetQueriesParams::default();
            if let Some(id) = worksheet_id {
                params = params.with_worksheet_id(id);
            }
            let queries = history_store
                .get_queries(params)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_iter()
                .map(|q| q.id.to_string())
                .collect::<Vec<_>>();
            let query_id = get_query_by_index(&queries, index).unwrap_or_default();
            Ok::<String, DataFusionError>(query_id)
        })
        .map_err(|e| DataFusionError::Execution(e.to_string()))??;
        Ok(utf8_val(&id))
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn cancel_query(&self, query_id: String) -> Result<ScalarValue> {
        let Ok(query_uuid) = Uuid::from_str(&query_id) else {
            return Ok(utf8_val("Invalid UUID."));
        };

        let query_id = QueryRecordId::from(query_uuid);
        if self
            .running_queries
            .abort(AbortQuery::ByQueryId(query_id))
            .is_err()
        {
            Ok(utf8_val(
                "Identified SQL statement is not currently executing.",
            ))
        } else {
            Ok(utf8_val(format!(
                "query [{}] terminated.",
                query_id.as_uuid()
            )))
        }
    }
}
struct ExprRewriter<'a> {
    rewriter: &'a SessionContextExprRewriter,
}

impl TreeNodeRewriter for ExprRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::ScalarFunction(fun) = &expr {
            let name = fun.name().to_lowercase();
            let scalar_value = match name.as_str() {
                "current_database" => Some(utf8_val(&self.rewriter.database)),
                "current_schema" => Some(utf8_val(&self.rewriter.schema)),
                "current_warehouse" => Some(utf8_val(&self.rewriter.warehouse)),
                "current_role_type" => Some(utf8_val("ROLE")),
                "current_role" => Some(utf8_val("default")),
                "current_version" => Some(utf8_val(&self.rewriter.version)),
                "current_client" => Some(utf8_val(format!("Embucket {}", &self.rewriter.version))),
                "current_session" => Some(utf8_val(&self.rewriter.session_id)),
                "last_query_id" => {
                    let index = match fun.args.first() {
                        Some(Expr::Literal(value, _)) => value.clone().try_into().unwrap_or(-1),
                        _ => -1,
                    };
                    Some(self.rewriter.last_query_id(index)?)
                }
                "current_ip_address" => Some(utf8_val(
                    self.rewriter
                        .query_context
                        .ip_address
                        .clone()
                        .unwrap_or_default(),
                )),
                "current_schemas" => Some(list_val(&self.rewriter.schemas)),
                "system$cancel_query" => {
                    let query_id = match fun.args.first() {
                        Some(Expr::Literal(ScalarValue::Utf8(Some(value)))) => value.clone(),
                        _ => String::default(),
                    };
                    Some(self.rewriter.cancel_query(query_id)?)
                }
                _ => None,
            };
            if let Some(value) = scalar_value {
                return Ok(Transformed::yes(
                    Expr::Literal(value, None).alias(fun.name()),
                ));
            }
        }

        Ok(Transformed::no(expr))
    }
}

fn utf8_val(val: impl Into<String>) -> ScalarValue {
    ScalarValue::Utf8(Some(val.into()))
}

fn list_val(items: &[String]) -> ScalarValue {
    let mut builder = ListBuilder::new(StringBuilder::new());
    let values_builder = builder.values();
    for item in items {
        values_builder.append_value(item);
    }
    builder.append(true);
    let array = builder.finish();
    ScalarValue::List(Arc::new(ListArray::from(array)))
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
