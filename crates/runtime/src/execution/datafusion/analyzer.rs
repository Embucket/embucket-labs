use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Cast, Expr};

#[derive(Debug)]
pub struct IcebergTypesAnalyzer;

/// Rewrites expressions in the logical plan to cast `UInt64` columns to `Int64`
/// for compatibility with the Apache Iceberg format.
///
/// Currently, Apache Iceberg does not support the `UInt64` data type. When creating
/// a table, Iceberg coerces such fields to `Int64`. This mismatch leads to schema
/// validation errors during insert or write operations if the input data retains
/// the `UInt64` type.
///
/// This rule transforms the logical plan by replacing any reference to `UInt64`
/// columns with an explicit `CAST(... AS Int64)` expression. This ensures the input
/// data conforms to the table schema expected by Iceberg.
///
/// ⚠️ Note: Converting from `UInt64` to `Int64` may result in **integer overflow**
/// if the original values exceed the maximum value of `Int64` (i.e., values > `i64::MAX`).
/// This rule does not check for overflow at compile time and assumes that upstream logic
/// ensures value safety or that overflow behavior is acceptable in the target context.
///
/// This is a temporary workaround until the Iceberg specification officially
/// supports `UInt64` in a future release.
///
/// The transformation is applied as an `AnalyzerRule`, ensuring that type coercion
/// occurs early in the planning pipeline.
impl AnalyzerRule for IcebergTypesAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        let LogicalPlan::Dml(_) = plan else {
            return Ok(plan);
        };
        plan.transform_down_with_subqueries(analyze_internal).data()
    }

    fn name(&self) -> &'static str {
        "IcebergTypesAnalyzer"
    }
}

fn analyze_internal(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    let schema = plan.schema().clone();
    let name_preserver = NamePreserver::new(&plan);

    let new_plan = plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);

        let transformed_expr = expr.transform_up(|e| {
            if let Expr::Column(col) = &e {
                if let Ok(field) = schema.field_with_unqualified_name(&col.name) {
                    if field.data_type() == &DataType::UInt64 {
                        let casted = Expr::Cast(Cast {
                            expr: Box::new(Expr::Column(col.clone())),
                            data_type: DataType::Int64,
                        })
                        .alias(col.name.clone());

                        return Ok(Transformed::yes(casted));
                    }
                }
            }
            Ok(Transformed::no(e))
        })?;

        Ok(transformed_expr.update_data(|data| original_name.restore(data)))
    })?;

    Ok(new_plan)
}
