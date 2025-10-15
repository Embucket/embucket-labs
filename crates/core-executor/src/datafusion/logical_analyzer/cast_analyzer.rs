use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Cast, Expr, ReturnFieldArgs, ScalarUDF, TryCast};
use embucket_functions::conversion::to_array::ToArrayFunc;
use embucket_functions::conversion::to_date::ToDateFunc;
use embucket_functions::conversion::to_decimal::ToDecimalFunc;
use embucket_functions::conversion::to_timestamp::ToTimestampFunc;
use embucket_functions::session_params::SessionParams;
use std::fmt::Debug;
use std::sync::Arc;

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

    fn to_timestamp_udf(&self, try_mode: bool) -> ScalarUDF {
        let name = if try_mode {
            "try_to_timestamp".to_string()
        } else {
            "to_timestamp".to_string()
        };
        ScalarUDF::from(ToTimestampFunc::new(
            try_mode,
            name,
            self.session_params.clone(),
        ))
    }

    fn analyze_internal(&self, plan: &LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(plan);
        let new_plan = plan.clone().map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);

            let transformed_expr = expr.transform_up(|e| match &e {
                Expr::Cast(cast) => self.rewrite_cast_to(&cast.data_type, &cast.expr, &e, false),
                Expr::TryCast(try_cast) => self.rewrite_cast_to(&try_cast.data_type, &try_cast.expr, &e, true),
                _ => Ok(Transformed::no(e)),
            })?;

            Ok(transformed_expr.update_data(|data| original_name.restore(data)))
        })?;
        Ok(new_plan)
    }

    fn rewrite_cast_to(
        &self,
        data_type: &DataType,
        expr: &Expr,
        original_expr: &Expr,
        try_mode: bool,
    ) -> DFResult<Transformed<Expr>> {
        match data_type.clone() {
            DataType::Date32 => Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(ToDateFunc::new(try_mode))),
                args: vec![expr.clone()],
            }))),
            DataType::Timestamp(_, _) => {
                if let Some(ts_cast) =
                    self.rewrite_timestamp_cast(data_type, expr.clone(), try_mode)?
                {
                    return Ok(ts_cast);
                }
                Ok(Transformed::no(original_expr.clone()))
            }
            DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _) => {
                Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(ScalarUDF::from(ToArrayFunc::new())),
                    args: vec![expr.clone()],
                })))
            }
            data_type @ (DataType::Decimal128(_, _) | DataType::Int32 | DataType::Int64) => {
                Self::rewrite_numeric_cast(expr, data_type, try_mode)
            }
            _ => Ok(Transformed::no(original_expr.clone())),
        }
    }

    // TODO: support `to_double` instead of `to_decimal`
    #[allow(clippy::unnecessary_wraps)]
    fn rewrite_numeric_cast(
        expr: &Expr,
        data_type: DataType,
        try_mode: bool,
    ) -> DFResult<Transformed<Expr>> {
        let func = Arc::new(ScalarUDF::from(ToDecimalFunc::new(try_mode)));
        let new_expr = if let DataType::Decimal128(precision, scale) = data_type {
            Expr::ScalarFunction(ScalarFunction {
                func,
                args: vec![
                    expr.clone(),
                    Expr::Literal(ScalarValue::UInt8(Some(precision)), None),
                    Expr::Literal(ScalarValue::Int8(Some(scale)), None),
                ],
            })
        } else {
            let internal = Expr::ScalarFunction(ScalarFunction {
                func,
                args: vec![expr.clone()],
            });
            if try_mode {
                Expr::TryCast(TryCast {
                    expr: Box::new(internal),
                    data_type,
                })
            } else {
                Expr::Cast(Cast {
                    expr: Box::new(internal),
                    data_type,
                })
            }
        };
        Ok(Transformed::yes(new_expr))
    }

    fn rewrite_timestamp_cast(
        &self,
        data_type: &DataType,
        expr: Expr,
        try_mode: bool,
    ) -> DFResult<Option<Transformed<Expr>>> {
        if let Expr::Literal(ScalarValue::Utf8(Some(v)), _) = expr.clone() {
            let udf = self.to_timestamp_udf(try_mode);

            // Infer the return field of the UDF for the given literal
            let return_field = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::new(Field::new("arg0", DataType::Utf8, true))],
                scalar_arguments: &[Some(&ScalarValue::Utf8(Some(v)))],
            })?;
            let func_return_type = return_field.data_type().clone();
            let mut expr = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(udf),
                args: vec![expr],
            });

            // Wrap the UDF result with a CAST only if its return type differs from the target type
            if func_return_type != *data_type {
                // Special case: if the UDF return type is Timestamp(Microsecond, _),
                // it means the literal had out-of-range nanoseconds.
                // In that case, we should keep the UDF's return type instead of forcing the target type.
                let final_type = match func_return_type {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => func_return_type,
                    _ => data_type.clone(),
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
            .data()?
            .recompute_schema()
    }

    fn name(&self) -> &'static str {
        "CastAnalyzer"
    }
}
