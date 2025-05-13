mod eliminate_empty_datasource_exec;
mod remove_exec_above_empty;

use datafusion::physical_optimizer::optimizer::{PhysicalOptimizer, PhysicalOptimizerRule};
use std::sync::Arc;

use super::physical_optimizer::eliminate_empty_datasource_exec::EliminateEmptyDataSourceExec;
use super::physical_optimizer::remove_exec_above_empty::RemoveExecAboveEmpty;

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
