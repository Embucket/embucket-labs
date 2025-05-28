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

            let scalar = match name.as_str() {
                "current_database" => Some(self.rewriter.database.clone()),
                "current_schema" => Some(self.rewriter.schema.clone()),
                "current_warehouse" => Some(self.rewriter.warehouse.clone()),
                "current_role_type" => Some("ROLE".to_string()),
                "current_role" => Some("default".to_string()),
                "current_version" => Some(env!("CARGO_PKG_VERSION").to_string()),
                "current_client" => Some(format!("Embucket {}", env!("CARGO_PKG_VERSION"))),
                "current_session" => Some(self.rewriter.session_id.to_string()),
                _ => None,
            };

            if let Some(text) = scalar {
                return Ok(Transformed::yes(
                    Expr::Literal(ScalarValue::Utf8(Some(text))).alias(fun.name()),
                ));
            }

            if name == "current_schemas" {
                let mut builder = ListBuilder::new(StringBuilder::new());
                let values_builder = builder.values();

                for schema in &self.rewriter.schemas {
                    values_builder.append_value(schema);
                }

                builder.append(true);
                let array = builder.finish();
                let list = ScalarValue::List(Arc::new(ListArray::from(array)));

                return Ok(Transformed::yes(Expr::Literal(list).alias(fun.name())));
            }
        }

        Ok(Transformed::no(expr))
    }
}
