use datafusion::arrow::datatypes::DataType::{Boolean, Int32, Int64, Utf8};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, ScalarUDF};
use embucket_functions::conversion::{ToBooleanFunc, ToDecimalFunc};
use std::fmt::Debug;
use std::sync::Arc;

/// Custom type coercion rule used to extend built-in type coercion logic.
/// In some cases we need custom handling to call scalar functions instead of direct casting.
///
/// This rule traverses the logical plan and rewrites expressions where the
/// default type coercion is not sufficient.
#[derive(Default, Debug)]
pub struct CustomTypeCoercionRewriter {}

impl CustomTypeCoercionRewriter {
    pub const fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for CustomTypeCoercionRewriter {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| analyze_internal(&plan))
            .data()
    }

    fn name(&self) -> &'static str {
        "custom_type_coercion"
    }
}

fn analyze_internal(plan: &LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let schema = merge_schema(&plan.inputs());

    let name_preserver = NamePreserver::new(plan);
    let new_plan = plan.clone().map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);

        let transformed_expr = expr.transform_up(|e| match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (left, right) = coerce_binary_op(*left, &schema, op, *right, &schema)?;
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left),
                    op,
                    Box::new(right),
                ))))
            }
            _ => Ok(Transformed::no(e)),
        })?;

        Ok(transformed_expr.update_data(|data| original_name.restore(data)))
    })?;
    Ok(new_plan)
}

fn coerce_binary_op(
    left: Expr,
    left_schema: &DFSchema,
    op: Operator,
    right: Expr,
    right_schema: &DFSchema,
) -> DFResult<(Expr, Expr)> {
    let (lhs_type, rhs_type) = (&left.get_type(left_schema)?, &right.get_type(right_schema)?);
    if lhs_type.equals_datatype(rhs_type) && op != Operator::Divide {
        return Ok((left, right));
    }

    match (lhs_type, op, rhs_type) {
        (Boolean, _, Utf8) => {
            let right_expr = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(ToBooleanFunc::new(true))),
                args: vec![right],
            });
            Ok((left, right_expr))
        }
        (Int64 | Int32, Operator::Divide, Int64 | Int32) => {
            //Snowflake looks like Decimal(38, 12) for division
            let left_expr = to_decimal_func(left, 38, 12);
            let right_expr = to_decimal_func(right, 38, 12);
            Ok((left_expr, right_expr))
        }
        _ => Ok((left, right)),
    }
}

fn to_decimal_func(value: Expr, precision: u8, scale: i8) -> Expr {
    let precision = Expr::Literal(ScalarValue::UInt8(Some(precision)), None);
    let scale = Expr::Literal(ScalarValue::Int8(Some(scale)), None);
    Expr::ScalarFunction(ScalarFunction {
        func: Arc::new(ScalarUDF::from(ToDecimalFunc::new(false))),
        args: vec![value, precision, scale],
    })
}
