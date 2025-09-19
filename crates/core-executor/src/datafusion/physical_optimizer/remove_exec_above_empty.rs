use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::aggregates::AggregateExec;

/// A physical optimizer rule that removes any execution plan node
/// whose all inputs are `EmptyExec`. Since an `EmptyExec` always
/// returns no rows, operations like `SortExec`, `FilterExec`,
/// `ProjectionExec`, `JoinExec` (with both sides empty), etc., can be
/// safely replaced with a single `EmptyExec`.
///
/// However, aggregate operations (like `AggregateExec`) are preserved even
/// when their inputs are empty, because aggregate functions should return
/// results even on empty tables (e.g., COUNT(*) should return 0).
///
/// This helps avoid unnecessary execution overhead and simplifies the final plan.
///
/// # Examples
///
/// Unary (e.g., SortExec):
/// ```text
/// SortExec
///   └── EmptyExec
/// => EmptyExec
/// ```
///
/// Binary (e.g., JoinExec):
/// ```text
/// HashJoinExec
///   ├── EmptyExec
///   └── EmptyExec
/// => EmptyExec
/// ```
#[derive(Default, Debug)]
pub struct RemoveExecAboveEmpty;

impl RemoveExecAboveEmpty {
    pub const fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for RemoveExecAboveEmpty {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            // Skip EmptyExec itself and do not replace aggregate operations with EmptyExec because they should
            // still return results even on empty tables (e.g., COUNT(*) should return 0)
            if plan.as_any().is::<EmptyExec>() || contains_aggregates(&plan) {
                return Ok(Transformed::no(plan));
            }

            let inputs = plan.as_ref().children();
            if inputs.is_empty() {
                return Ok(Transformed::no(plan));
            }

            // Replace the current node with EmptyExec if all inputs are EmptyExec
            let all_empty = inputs.iter().all(|input| input.as_any().is::<EmptyExec>());
            if all_empty {
                return Ok(Transformed::yes(Arc::new(EmptyExec::new(plan.schema()))));
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "RemoveExecAboveEmpty"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Check if the execution plan contains any aggregate operations
fn contains_aggregates(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().is::<AggregateExec>() {
        return true;
    }

    plan.children()
        .iter()
        .any(|child| contains_aggregates(child))
}

#[cfg(test)]
mod tests {
    use crate::datafusion::physical_optimizer::remove_exec_above_empty::RemoveExecAboveEmpty;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalExprRef, PhysicalSortExpr};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{JoinType, Result};
    use datafusion_physical_plan::joins::{
        HashJoinExec, JoinOn, NestedLoopJoinExec, PartitionMode,
    };
    use datafusion_physical_plan::limit::GlobalLimitExec;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn empty_exec() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    #[tokio::test]
    async fn test_remove_sort_above_empty() -> Result<()> {
        let sort_exprs = LexOrdering::from([PhysicalSortExpr {
            expr: col("id", &schema())?,
            options: SortOptions::default(),
        }]);
        let sort = Arc::new(SortExec::new(sort_exprs, empty_exec()));

        let optimizer = RemoveExecAboveEmpty::new();
        let optimized = optimizer.optimize(sort, &ConfigOptions::new())?;
        assert!(
            optimized.as_any().is::<EmptyExec>(),
            "Plan was not optimized to EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_filter_above_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Boolean,
            false,
        )]));
        let predicate = col("id", &schema)?;
        let filter = Arc::new(FilterExec::try_new(
            predicate,
            Arc::new(EmptyExec::new(schema)),
        )?);

        let rule = RemoveExecAboveEmpty::new();
        let optimized = rule.optimize(filter, &ConfigOptions::default())?;
        assert!(
            optimized.as_any().is::<EmptyExec>(),
            "Plan was not optimized to EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_projection_above_empty() -> Result<()> {
        let proj_exprs = vec![(col("id", &schema())?, "id".to_string())];
        let proj = Arc::new(ProjectionExec::try_new(proj_exprs, empty_exec())?);

        let rule = RemoveExecAboveEmpty::new();
        let optimized = rule.optimize(proj, &ConfigOptions::default())?;
        assert!(
            optimized.as_any().is::<EmptyExec>(),
            "Plan was not optimized to EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_exec_above_empty_limit_exec() -> Result<()> {
        let global_limit_exec = Arc::new(GlobalLimitExec::new(empty_exec(), 10, None));

        let rule = RemoveExecAboveEmpty::new();
        let optimized = rule.optimize(global_limit_exec, &ConfigOptions::default())?;
        assert!(
            optimized.as_any().downcast_ref::<EmptyExec>().is_some(),
            "LimitExec should be removed above EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::as_conversions)]
    async fn test_remove_exec_above_empty_hash_join_exec() -> Result<()> {
        let on: JoinOn = vec![(
            Arc::new(Column::new("id", 0)) as PhysicalExprRef,
            Arc::new(Column::new("id", 0)) as PhysicalExprRef,
        )];
        let hash_join_exec = Arc::new(HashJoinExec::try_new(
            empty_exec(),
            empty_exec(),
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNull,
        )?);

        let rule = RemoveExecAboveEmpty::new();
        let optimized = rule.optimize(hash_join_exec, &ConfigOptions::default())?;
        assert!(
            optimized.as_any().downcast_ref::<EmptyExec>().is_some(),
            "HashJoinExec should be removed above EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_exec_above_empty_nested_loop_join_exec() -> Result<()> {
        let nested_loop_join_exec = Arc::new(NestedLoopJoinExec::try_new(
            empty_exec(),
            empty_exec(),
            None,
            &JoinType::Inner,
            None,
        )?);

        let rule = RemoveExecAboveEmpty::new();
        let optimized = rule.optimize(nested_loop_join_exec, &ConfigOptions::default())?;
        assert!(
            optimized.as_any().downcast_ref::<EmptyExec>().is_some(),
            "NestedLoopJoinExec should be removed above EmptyExec"
        );
        Ok(())
    }
}
