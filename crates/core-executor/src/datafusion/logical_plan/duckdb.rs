use std::{hash::Hasher, sync::Arc};

use datafusion::logical_expr_common::dyn_eq::DynHash;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNode};

#[derive(Debug, Clone)]
struct DuckDBLogicalNode {
    query: String,
    schema: DFSchemaRef,
}

impl UserDefinedLogicalNode for DuckDBLogicalNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "DuckDBNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
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
        write!(f, "DuckDBNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        if !inputs.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "MergeIntoSink requires exactly one input".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            query: self.query.clone(),
            schema: self.schema.clone(),
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        "DuckDBNode".dyn_hash(state);
        self.query.dyn_hash(state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.query == other.query && self.schema == other.schema
        } else {
            false
        }
    }

    fn dyn_ord(&self, _other: &dyn UserDefinedLogicalNode) -> Option<std::cmp::Ordering> {
        None
    }
}
