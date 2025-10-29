use std::hash::Hasher;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::physical_expr_common::physical_expr::DynHash;
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNode};
use datafusion_iceberg::DataFusionTable;

#[derive(Debug, Clone)]
pub struct DuckDBSubstitute {
    pub input: Arc<LogicalPlan>,
    pub schema: DFSchemaRef,
}

impl DuckDBSubstitute {
    pub fn new(input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Self {
        Self { input, schema }
    }
}

impl UserDefinedLogicalNode for DuckDBSubstitute {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DuckDBSubstitute"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        &self.schema
    }

    fn check_invariants(&self, _check: InvariantLevel) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBSubsitution")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        if inputs.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "DuckDBSubstitute requires exactly one input".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            input: Arc::new(inputs.into_iter().next().ok_or(
                datafusion_common::DataFusionError::Internal(
                    "DuckDBSubstitute requires exactly one input".to_string(),
                ),
            )?),
            schema: self.schema.clone(),
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        "DuckDBSubstitute".dyn_hash(state);
        self.input.dyn_hash(state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.input == other.input && self.schema == other.schema
        } else {
            false
        }
    }

    fn dyn_ord(&self, _other: &dyn UserDefinedLogicalNode) -> Option<std::cmp::Ordering> {
        None
    }
}