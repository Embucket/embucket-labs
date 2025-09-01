use arrow_schema::TimeUnit;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Cast, Expr, ReturnTypeArgs, ScalarUDF};
use embucket_functions::conversion::to_date::ToDateFunc;
use embucket_functions::conversion::to_timestamp::ToTimestampFunc;
use embucket_functions::session_params::SessionParams;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use embucket_functions::conversion::to_array::ToArrayFunc;

/// Rewrites expressions in the logical plan with explicit casts `...::*` or `CAST(... AS *)`
/// as an `TO_*` function call, where `*` is the `DataType` and corresponding function name respectively.
///
/// Currently supported types:
/// - `...::DATE` or `CAST(... AS DATE)` -> `to_date(...)` this allows for expression like `SELECT
/// '03-April-2024'::DATE;` to be valid with result `2024-04-03`.
/// - `Ut8String:TIMESTAMP` or `CAST(Ut8String AS TIMESTAMP)` -> `to_timestamp(...)` this allows for expression like `SELECT
///  '2025-12-31 00:00:00.000'::TIMESTAMP;` to be valid with result `2025-12-31 00:00:00.000`.
#[derive(Debug, Default)]
pub struct CastAnalyzer {
    session_params: Arc<SessionParams>,
}

impl CastAnalyzer {
    #[must_use]
    pub const fn new(session_params: Arc<SessionParams>) -> Self {
        Self { session_params }
    }

    fn to_timestamp_udf(&self) -> ScalarUDF {
        ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            self.session_params.clone(),
        ))
    }

    fn analyze_internal(&self, plan: &LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(plan);
        let new_plan = plan.clone().map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);

            let transformed_expr = expr.transform_up(|e| {
                if let Expr::Cast(cast) = &e {
                    match &cast.data_type {
                        DataType::Date32 => {
                            return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                                func: Arc::new(ScalarUDF::from(ToDateFunc::new(false))),
                                args: vec![cast.expr.deref().clone()],
                            })));
                        }
                        DataType::Timestamp(_, _) => {
                            if let Some(ts_cast) = self.rewrite_timestamp_cast(cast)? {
                                return Ok(ts_cast);
                            }
                        }
                        DataType::List(_) | DataType::ListView(_) | DataType::LargeList(_) | DataType::LargeListView(_) | DataType::FixedSizeList(_, _) => {
                            return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                                func: Arc::new(ScalarUDF::from(ToArrayFunc::new())),
                                args: vec![cast.expr.deref().clone()],
                            })));
                        }
                        _ => return Ok(Transformed::no(e)),
                    }
                }
                Ok(Transformed::no(e))
            })?;

            Ok(transformed_expr.update_data(|data| original_name.restore(data)))
        })?;
        Ok(new_plan)
    }

    fn rewrite_timestamp_cast(&self, cast: &Cast) -> DFResult<Option<Transformed<Expr>>> {
        if let Expr::Literal(ScalarValue::Utf8(Some(v))) = &*cast.expr {
            let udf = self.to_timestamp_udf();

            // Infer the return type of the UDF for the given literal
            let return_info = udf.return_type_from_args(ReturnTypeArgs {
                arg_types: &[DataType::Utf8],
                scalar_arguments: &[Some(&ScalarValue::Utf8(Some(v.clone())))],
                nullables: &[],
            })?;
            let func_return_type = return_info.return_type().clone();
            let mut expr = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(udf),
                args: vec![cast.expr.deref().clone()],
            });

            // Wrap the UDF result with a CAST only if its return type differs from the target type
            if func_return_type != cast.data_type {
                // Special case: if the UDF return type is Timestamp(Microsecond, _),
                // it means the literal had out-of-range nanoseconds.
                // In that case, we should keep the UDF's return type instead of forcing the target type.
                let final_type = match func_return_type {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => func_return_type,
                    _ => cast.data_type.clone(),
                };
                expr = Expr::Cast(Cast {
                    expr: Box::new(expr),
                    data_type: final_type,
                });
            }
            return Ok(Some(Transformed::yes(expr)));
        }
        Ok(None)
    }
}

impl AnalyzerRule for CastAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| self.analyze_internal(&plan))
            .data()?.recompute_schema()
    }

    fn name(&self) -> &'static str {
        "CastAnalyzer"
    }
}
