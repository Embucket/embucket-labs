use std::hash::Hasher;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::physical_expr_common::physical_expr::DynHash;
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNode};
use datafusion_iceberg::DataFusionTable;

#[derive(Debug, Clone)]
// The MergeIntoCOWSink performs the final writing step of the "MERGE INTO" statement. It assumes
// that the typical Join of the target and source tables is already performed and that the Recordbatches
// have a particular form. As such the Recordbatches must contain a "__target_exists" and
// "__source_exists" column that indicate whether the given row exists in the target and source
// tables respectivaly. Additionally, it requires a "__data_file_path" column to keep track of which files to overwrite.
// It then writes the resulting data to parquet files and updates the target
// table accordingly.
pub struct MergeIntoCOWSink {
    pub input: Arc<LogicalPlan>,
    pub target: DataFusionTable,
    pub schema: DFSchemaRef,
}

impl MergeIntoCOWSink {
    pub fn new(
        input: Arc<LogicalPlan>,
        target: DataFusionTable,
    ) -> datafusion_common::Result<Self> {
        let field = Field::new("number of rows updated", DataType::Int64, false);
        let schema = DFSchema::new_with_metadata(
            vec![(None, Arc::new(field))],
            std::collections::HashMap::new(),
        )?;

        Ok(Self {
            input,
            target,
            schema: Arc::new(schema),
        })
    }
}

impl UserDefinedLogicalNode for MergeIntoCOWSink {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "MergeIntoSink"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        &self.schema
    }

    fn check_invariants(
        &self,
        _check: InvariantLevel,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeIntoSink")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        if inputs.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "MergeIntoSink requires exactly one input".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            input: Arc::new(inputs.into_iter().next().ok_or(
                datafusion_common::DataFusionError::Internal(
                    "MergeIntoSink requires exactly one input".to_string(),
                ),
            )?),
            target: self.target.clone(),
            schema: self.schema.clone(),
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        "MergeIntoSink".dyn_hash(state);
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
