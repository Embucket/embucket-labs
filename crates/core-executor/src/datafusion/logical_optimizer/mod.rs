use crate::datafusion::logical_optimizer::split_ordered_aggregates::SplitOrderedAggregates;
use datafusion::optimizer::OptimizerRule;
use datafusion_federation::default_optimizer_rules;
use std::sync::Arc;

pub mod split_ordered_aggregates;

/// Returns a list of logical optimizer rules including custom rules.
#[must_use]
pub fn logical_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = default_optimizer_rules();
    rules.push(Arc::new(SplitOrderedAggregates::new()));
    rules
}
