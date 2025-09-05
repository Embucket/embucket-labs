use arrow_schema::{Field, Schema};
use datafusion::arrow::array::{Array, ArrayData, ListArray};
use datafusion::arrow::buffer::Buffer;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::projection::ProjectionExec;
use std::fmt::Debug;
use std::sync::Arc;

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
                    let new_exprs: DFResult<Vec<(Arc<dyn PhysicalExpr>, String)>> = proj
                        .expr()
                        .iter()
                        .enumerate()
                        .map(|(i, (expr, _))| {
                            let target_field = Arc::new(self.target_schema.field(i).clone());
                            if let Some(lit) = expr.as_any().downcast_ref::<Literal>()
                                && let Some(new_lit) =
                                    rebuild_list_literal(lit, self.target_schema.field(i))
                            {
                                return Ok((
                                    Arc::new(new_lit) as Arc<dyn PhysicalExpr>,
                                    target_field.name().to_string(),
                                ));
                            }
                            Ok((expr.clone(), target_field.name().to_string()))
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
