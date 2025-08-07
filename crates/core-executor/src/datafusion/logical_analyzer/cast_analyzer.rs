use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, ScalarUDF};
use embucket_functions::conversion::to_date::ToDateFunc;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

/// Rewrites expressions in the logical plan with explicit casts `...::*` or `CAST(... AS *)`
/// as an `TO_*` function call, where `*` is the `DataType` and corresponding function name respectively.
///
/// Currently supported types:
/// - `...::DATE` or `CAST(... AS DATE)` -> `to_date(...)` this allows for expression like `SELECT
/// '03-April-2024'::DATE;` to be valid with result `2024-04-03`.
#[derive(Debug, Default)]
pub struct CastAnalyzer;

impl CastAnalyzer {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
    fn analyze_internal(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(&plan);

        let new_plan = plan.map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);

            let transformed_expr = expr.transform_up(|e| {
                if let Expr::Cast(cast) = &e
                    && cast.data_type == DataType::Date32
                {
                    return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                        //TODO: should we somehow provide this function from session context?
                        func: Arc::new(ScalarUDF::from(ToDateFunc::new(false))),
                        args: vec![cast.expr.deref().clone()],
                    })));
                }
                Ok(Transformed::no(e))
            })?;

            Ok(transformed_expr.update_data(|data| original_name.restore(data)))
        })?;

        Ok(new_plan)
    }
}

impl AnalyzerRule for CastAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        //TODO: What plans should it concern? Projection what else? Or all of them?
        plan.transform_down_with_subqueries(Self::analyze_internal)
            .data()
    }

    fn name(&self) -> &'static str {
        "CastAnalyzer"
    }
}
