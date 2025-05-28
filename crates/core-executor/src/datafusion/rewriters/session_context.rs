use datafusion::arrow::array::{ListArray, ListBuilder, StringBuilder};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::Alias;
use std::sync::Arc;

pub struct SessionContextExprRewriter {
    pub database: String,
    pub schema: String,
    pub schemas: Vec<String>,
    pub warehouse: String,
}

impl SessionContextExprRewriter {
    fn rewrite_expr(&self, expr: Expr) -> Expr {
        match expr {
            Expr::Alias(alias) => {
                let rewritten_inner = self.rewrite_expr(*alias.expr);
                Expr::Alias(Alias {
                    expr: Box::new(rewritten_inner),
                    relation: alias.relation,
                    name: alias.name,
                    metadata: alias.metadata,
                })
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_database" => {
                Expr::Literal(self.database.clone().into()).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_schema" => {
                Expr::Literal(self.schema.clone().into()).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_schemas" => {
                let mut builder = ListBuilder::new(StringBuilder::new());
                let values_builder = builder.values();

                for schema in &self.schemas {
                    values_builder.append_value(schema);
                }
                builder.append(true);
                let list_scalar = ScalarValue::List(Arc::new(ListArray::from(builder.finish())));
                Expr::Literal(list_scalar).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_warehouse" => {
                Expr::Literal(self.warehouse.clone().into()).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_role_type" => {
                Expr::Literal("ROLE".into()).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_role" => {
                Expr::Literal("default".into()).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_version" => {
                let version = env!("CARGO_PKG_VERSION");
                Expr::Literal(version.into()).alias(fun.name())
            }
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_client" => {
                let version = format!("Embucket {}", env!("CARGO_PKG_VERSION"));
                Expr::Literal(version.into()).alias(fun.name())
            }
            _ => expr,
        }
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
