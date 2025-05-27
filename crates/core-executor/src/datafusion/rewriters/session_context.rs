use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion_common::Result;
use datafusion_expr::expr::Alias;

pub struct SessionContextExprRewriter {
    pub database: String,
    pub schema: String,
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
            Expr::ScalarFunction(fun) if fun.name().to_lowercase() == "current_warehouse" => {
                Expr::Literal(self.warehouse.clone().into()).alias(fun.name())
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
