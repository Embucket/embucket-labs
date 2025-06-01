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
                "current_version" => Some(utf8_val(env!("CARGO_PKG_VERSION"))),
                "current_client" => {
                    Some(utf8_val(format!("Embucket {}", env!("CARGO_PKG_VERSION"))))
                }
                "current_session" => Some(utf8_val(&self.rewriter.session_id)),
                "current_schemas" => Some(list_val(&self.rewriter.schemas)),
                _ => None,
            };

            if let Some(value) = scalar_value {
                return Ok(Transformed::yes(Expr::Literal(value).alias(fun.name())));
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
