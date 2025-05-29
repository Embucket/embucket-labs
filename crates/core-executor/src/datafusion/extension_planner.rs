use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;

#[derive(Debug, Default)]
pub struct CustomExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for CustomExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn datafusion::physical_planner::PhysicalPlanner,
        _node: &dyn datafusion_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&datafusion_expr::LogicalPlan],
        _physical_inputs: &[std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>],
        _session_state: &datafusion::execution::SessionState,
    ) -> datafusion_common::Result<
        Option<std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>>,
    > {
        Ok(None)
    }
}
