mod eliminate_empty_datasource_exec;
pub mod list_field_metadata;
mod remove_exec_above_empty;

use super::physical_optimizer::eliminate_empty_datasource_exec::EliminateEmptyDataSourceExec;
use super::physical_optimizer::list_field_metadata::ListFieldMetadataRule;
use super::physical_optimizer::remove_exec_above_empty::RemoveExecAboveEmpty;
use arrow_schema::Schema;
use datafusion::physical_optimizer::optimizer::{PhysicalOptimizer, PhysicalOptimizerRule};
use std::sync::Arc;

/// Returns a list of physical optimizer rules including custom rules.
#[must_use]
pub fn physical_optimizer_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
        Arc::new(EliminateEmptyDataSourceExec::new()),
        Arc::new(RemoveExecAboveEmpty::new()),
    ];

    // Append the default DataFusion optimizer rules
    rules.extend(PhysicalOptimizer::default().rules);

    rules
}

#[must_use]
pub fn runtime_physical_optimizer_rules(
    target_schema: Arc<Schema>,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![Arc::new(ListFieldMetadataRule::new(target_schema))]
}
