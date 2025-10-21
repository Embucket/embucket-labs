use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;

use crate::datafusion::{
    logical_plan::duckdb::DuckDBLogicalNode, physical_plan::duckdb::DuckDBPhysicalNode,
};

use super::{logical_plan::merge::MergeIntoCOWSink, physical_plan::merge::MergeIntoCOWSinkExec};

#[derive(Debug, Default)]
pub struct CustomExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for CustomExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn datafusion::physical_planner::PhysicalPlanner,
        node: &dyn datafusion_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&datafusion_expr::LogicalPlan],
        _physical_inputs: &[std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>],
        session_state: &datafusion::execution::SessionState,
    ) -> datafusion_common::Result<
        Option<std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>>,
    > {
        if let Some(merge) = node.as_any().downcast_ref::<MergeIntoCOWSink>() {
            let input = planner
                .create_physical_plan(&merge.input, session_state)
                .await?;
            Ok(Some(Arc::new(MergeIntoCOWSinkExec::new(
                merge.schema.clone(),
                input,
                merge.target.clone(),
            ))))
        } else if let Some(duckdb_node) = node.as_any().downcast_ref::<DuckDBLogicalNode>() {
            Ok(Some(Arc::new(DuckDBPhysicalNode::new(
                duckdb_node.query.clone(),
                duckdb_node.schema.clone(),
            ))))
        } else {
            Ok(None)
        }
    }
}
