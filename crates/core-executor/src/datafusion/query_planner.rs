use async_trait::async_trait;
use datafusion::{
    execution::context::QueryPlanner,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use std::{fmt, sync::Arc};

use super::extension_planner::CustomExtensionPlanner;

pub struct CustomQueryPlanner(DefaultPhysicalPlanner);

impl Default for CustomQueryPlanner {
    fn default() -> Self {
        Self(DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(CustomExtensionPlanner::default()),
        ]))
    }
}

#[async_trait]
impl QueryPlanner for CustomQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &datafusion_expr::LogicalPlan,
        session_state: &datafusion::execution::SessionState,
    ) -> datafusion_common::Result<std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>>
    {
        self.0
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

impl fmt::Debug for CustomQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EmbucketQueryPlanner")
    }
}
