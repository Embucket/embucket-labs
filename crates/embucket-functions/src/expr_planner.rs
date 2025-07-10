use crate::string_binary;
use datafusion_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult};

#[derive(Debug, Default)]
pub struct CustomExprPlanner;

impl ExprPlanner for CustomExprPlanner {
    fn plan_substring(
        &self,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<PlannerResult<Vec<Expr>>> {
        // Use our custom substr function instead of DataFusion's built-in one
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(string_binary::substr::get_udf(), args),
        )))
    }
}
