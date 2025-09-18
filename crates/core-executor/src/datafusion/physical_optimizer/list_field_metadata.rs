use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Array, ArrayData, ListArray};
use datafusion::arrow::buffer::Buffer;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use std::fmt::Debug;
use std::sync::Arc;

/// Physical optimizer rule to enforce consistent List field metadata in `ProjectionExec`.
///
/// In `DataFusion`, when projecting columns or literals that contain `ListArrays`, the
/// schema of the output may differ from the desired target schema. This can happen
/// if the `ListArray` was constructed with different field metadata (e.g., Parquet
/// field IDs) or if the element type differs slightly (Utf8 vs Int64, etc.).
///
/// The `ListFieldMetadataRule` rewrites `ProjectionExec` expressions to ensure:
/// 1. Literal `ListArrays` are rebuilt with the target field's schema (using `rebuild_list_literal`).
/// 2. Column expressions of List type are cast to the target field's List type if needed.
///
/// This prevents schema mismatches downstream, especially when writing to Parquet
/// or when further optimizations assume a consistent schema.
///
/// # Example
///
/// ```text
/// SELECT [1,2]::ARRAY as arr   -- literal list
/// SELECT arr FROM VALUES ([1,2]::ARRAY),([1,2]::ARRAY) AS t(arr)  -- column list
/// ```
/// Both cases will be adjusted so that the projected `ListArray` matches the target schema.
///
/// # Notes
///
/// - Only affects `ProjectionExec` nodes in the plan.
/// - Preserves original expressions if they already match the target schema.
/// - Supports both literals and column expressions.
/// - Rebuilds internal `ListArray` data for literals, preserving offsets, values, and null bitmaps.
#[derive(Debug)]
pub struct ListFieldMetadataRule {
    pub target_schema: Arc<Schema>,
}

impl ListFieldMetadataRule {
    #[must_use]
    pub const fn new(target_schema: Arc<Schema>) -> Self {
        Self { target_schema }
    }
}

#[allow(clippy::as_conversions)]
impl PhysicalOptimizerRule for ListFieldMetadataRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
                let proj_schema = proj.schema();

                if proj_schema.fields() != self.target_schema.fields() {
                    let new_exprs: DFResult<Vec<ProjectionExpr>> = proj
                        .expr()
                        .iter()
                        .map(|proj_expr| {
                            let expr = &proj_expr.expr;
                            let name = proj_expr.alias.clone();
                            let Ok(target_field) = self.target_schema.field_with_name(&name) else {
                                return Ok(ProjectionExpr::new(expr.clone(), name));
                            };

                            // If the expression is a literal containing a ListArray,
                            // attempt to rebuild the literal so that its internal ListArray
                            // matches the target field's data type (including metadata for Parquet compatibility).
                            if let Some(lit) = expr.as_any().downcast_ref::<Literal>()
                                && let Some(new_lit) = rebuild_list_literal(lit, target_field)
                            {
                                return Ok(ProjectionExpr::new(Arc::new(new_lit) as Arc<dyn PhysicalExpr>, name));
                            }

                            // If the expression is a List type but the element type or metadata
                            // differs from the target field, wrap the expression in a CastExpr
                            // to convert it to the target field's List type.
                            if let Ok(expr_type) = expr.data_type(&proj.input().schema())
                                && let (DataType::List(_), DataType::List(_)) =
                                    (expr_type, target_field.data_type())
                            {
                                let casted = Arc::new(CastExpr::new(
                                    expr.clone(),
                                    target_field.data_type().clone(),
                                    None,
                                ));
                                return Ok(ProjectionExpr::new(casted, name));
                            }

                            Ok(ProjectionExpr::new(expr.clone(), name))
                        })
                        .collect();

                    let new_proj = ProjectionExec::try_new(new_exprs?, proj.input().clone())?;
                    return Ok(Transformed::yes(Arc::new(new_proj)));
                }
            }
            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "ListFieldMetadataRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rebuild_list_literal(lit: &Literal, target_field: &Field) -> Option<Literal> {
    if let ScalarValue::List(array) = lit.value() {
        let offsets = array.offsets().clone().into_inner();
        let values = array.values().clone();
        let nulls = array.nulls().cloned();

        let list_data = ArrayData::builder(target_field.data_type().clone())
            .len(array.len())
            .add_buffer(Buffer::from(offsets))
            .add_child_data(values.into_data())
            .nulls(nulls)
            .build()
            .ok()?;
        let list_array = Arc::new(ListArray::from(list_data));
        Some(Literal::new(ScalarValue::List(list_array)))
    } else {
        None
    }
}
