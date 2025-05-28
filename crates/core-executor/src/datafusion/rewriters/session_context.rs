use datafusion::arrow::array::{ListArray, ListBuilder, StringBuilder};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use std::sync::Arc;

pub struct SessionContextExprRewriter {
    pub database: String,
    pub schema: String,
    pub schemas: Vec<String>,
    pub warehouse: String,
    pub session_id: String,
}

impl SessionContextExprRewriter {
    fn rewrite_expr(&self, expr: Expr) -> Expr {
        let mut rewriter = ExprRewriter { ctx: self };
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
}
struct ExprRewriter<'a> {
    ctx: &'a SessionContextExprRewriter,
}

impl TreeNodeRewriter for ExprRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let replacement = match &expr {
            Expr::ScalarFunction(fun) => {
                let name = fun.name().to_lowercase();
                let value = match name.as_str() {
                    "current_database" => Some(ScalarValue::Utf8(Some(self.ctx.database.clone()))),
                    "current_schema" => Some(ScalarValue::Utf8(Some(self.ctx.schema.clone()))),
                    "current_warehouse" => {
                        Some(ScalarValue::Utf8(Some(self.ctx.warehouse.clone())))
                    }
                    "current_role_type" => Some(ScalarValue::Utf8(Some("ROLE".to_string()))),
                    "current_role" => Some(ScalarValue::Utf8(Some("default".to_string()))),
                    "current_version" => Some(ScalarValue::Utf8(Some(
                        env!("CARGO_PKG_VERSION").to_string(),
                    ))),
                    "current_client" => Some(ScalarValue::Utf8(Some(format!(
                        "Embucket {}",
                        env!("CARGO_PKG_VERSION")
                    )))),
                    "current_session" => {
                        Some(ScalarValue::Utf8(Some(self.ctx.session_id.to_string())))
                    }
                    "current_schemas" => {
                        let mut builder = ListBuilder::new(StringBuilder::new());
                        let values_builder = builder.values();
                        for schema in &self.ctx.schemas {
                            values_builder.append_value(schema);
                        }
                        builder.append(true);
                        let array = builder.finish();
                        Some(ScalarValue::List(Arc::new(ListArray::from(array))))
                    }
                    _ => None,
                };

                if let Some(val) = value {
                    Ok(Transformed::yes(Expr::Literal(val).alias(fun.name())))
                } else {
                    Ok(Transformed::no(expr))
                }
            }
            _ => Ok(Transformed::no(expr)),
        };

        replacement
    }
}
