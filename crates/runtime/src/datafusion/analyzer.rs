use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::optimizer::AnalyzerRule;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{CreateCatalog, CreateCatalogSchema, DdlStatement, Extension, LogicalPlan};

#[derive(Debug)]
struct IcebergAnalyzerRule {}

impl AnalyzerRule for IcebergAnalyzerRule {
    
    fn analyze(&self, plan: LogicalPlan, _config: &datafusion::config::ConfigOptions) -> DFResult<LogicalPlan> {
        let transformed_plan = plan.transform(datafusion_iceberg::planner::iceberg_transform)?;
        Ok(transformed_plan.data)
    }
    
    fn name(&self) -> &str {
        "IcebergAnalyzerRule"
    }
}

pub struct IcebergCreateCatalog(CreateCatalog);
pub struct IcebergCreateCatalogSchema(CreateCatalogSchema);

