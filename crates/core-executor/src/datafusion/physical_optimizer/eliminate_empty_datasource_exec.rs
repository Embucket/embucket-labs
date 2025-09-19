use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;
use std::sync::Arc;

/// A physical optimizer rule that replaces empty `DataSourceExec`
/// nodes with an `EmptyExec`.
///
/// This optimization detects `DataSourceExec` instances that are
/// guaranteed to yield no data (i.e., they have no file groups and
/// their output partitioning is `UnknownPartitioning(0)`). Rather than
/// letting them propagate through the plan (e.g., into sorts or joins),
/// this rule replaces them directly with an `EmptyExec`, which short-circuits
/// further computation and avoids unnecessary execution.
///
/// This is especially useful in cases like:
/// - Filtered queries resulting in zero-matching partitions
/// - Static analysis detecting empty scans
/// - Preventing creation of empty files in sinks like Iceberg
///
/// # Example
///
/// Before:
/// ```text
/// SortExec
///   DataSourceExec (file_groups = [], partitioning = UnknownPartitioning(0))
/// ```
///
/// After:
/// ```text
/// SortExec
///   EmptyExec
/// ```
///
/// This rule is safe and conservative — it only applies when the
/// input is provably empty.
#[derive(Default, Debug)]
pub struct EliminateEmptyDataSourceExec {}

impl EliminateEmptyDataSourceExec {
    pub const fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EliminateEmptyDataSourceExec {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(source_exec) = plan.as_any().downcast_ref::<DataSourceExec>()
                && matches!(
                    source_exec.properties().output_partitioning(),
                    Partitioning::UnknownPartitioning(0)
                )
            {
                let schema = source_exec.schema();
                return Ok(Transformed::yes(Arc::new(EmptyExec::new(schema))));
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "EliminateEmptyDataSourceExec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_common::Result;
    use datafusion_common::config::TableParquetOptions;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    #[tokio::test]
    async fn test_eliminate_empty_data_source_exec_transformation() -> Result<()> {
        let object_store_url = ObjectStoreUrl::parse("s3://bucket")?;
        let file_source = Arc::new(ParquetSource::new(TableParquetOptions::default()));
        let file_scan_config = Arc::new(
            FileScanConfigBuilder::new(object_store_url, schema(), file_source)
                .with_file_groups(vec![])
                .build(),
        );
        let data_source_exec = Arc::new(DataSourceExec::new(file_scan_config));

        let rule = EliminateEmptyDataSourceExec::new();
        let optimized = rule.optimize(data_source_exec, &ConfigOptions::default())?;
        assert!(
            optimized.as_any().downcast_ref::<EmptyExec>().is_some(),
            "DataSourceExec with UnknownPartitioning(0) should be transformed to EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_no_transformation_for_non_empty_data_source_exec() -> Result<()> {
        let object_store_url = ObjectStoreUrl::parse("s3://bucket")?;
        let file_source = Arc::new(ParquetSource::new(TableParquetOptions::default()));
        let file_scan_config = Arc::new(
            FileScanConfigBuilder::new(object_store_url, schema(), file_source)
                .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new("path", 1)])])
                .build(),
        );
        let data_source_exec = Arc::new(DataSourceExec::new(file_scan_config));

        let rule = EliminateEmptyDataSourceExec::new();
        let optimized = rule.optimize(data_source_exec, &ConfigOptions::default())?;
        // The rule should not transform the DataSourceExec since it's not empty
        assert!(
            optimized
                .as_any()
                .downcast_ref::<DataSourceExec>()
                .is_some(),
            "DataSourceExec with non-empty partitioning should not be transformed to EmptyExec"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_eliminate_sort_exec_above_empty_data_source_exec() -> Result<()> {
        let object_store_url = ObjectStoreUrl::parse("s3://bucket")?;
        let file_source = Arc::new(ParquetSource::new(TableParquetOptions::default()));
        let file_scan_config = Arc::new(
            FileScanConfigBuilder::new(object_store_url, schema(), file_source)
                .with_file_groups(vec![])
                .build(),
        );
        let data_source_exec = Arc::new(DataSourceExec::new(file_scan_config));

        let sort_exprs = LexOrdering::from([PhysicalSortExpr {
            expr: col("id", &schema())?,
            options: SortOptions::default(),
        }]);
        let sort_exec = Arc::new(SortExec::new(sort_exprs, data_source_exec));

        let rule = EliminateEmptyDataSourceExec::new();
        let optimized = rule.optimize(sort_exec, &ConfigOptions::default())?;
        let sort = optimized
            .as_any()
            .downcast_ref::<SortExec>()
            .expect("Expected SortExec");

        let inner = sort.input();
        assert!(
            inner.as_any().downcast_ref::<EmptyExec>().is_some(),
            "Inner DataSourceExec should be replaced with EmptyExec"
        );

        Ok(())
    }
}
