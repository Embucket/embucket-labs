use std::sync::Arc;

use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_table_providers::sql::{
    db_connection_pool::duckdbpool::DuckDbConnectionPool, sql_provider_datafusion::get_stream,
};
use snafu::ResultExt;

use crate::{duckdb::execute_duck_db_explain, error};

#[derive(Debug, Clone)]
struct DuckDBPhysicalNode {
    query: String,
    schema: DFSchemaRef,
    properties: PlanProperties,
}

impl DuckDBPhysicalNode {
    fn new(query: String, schema: DFSchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::new(schema.as_ref().clone().into()));
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            query,
            schema,
            properties,
        }
    }
}

impl DisplayAs for DuckDBPhysicalNode {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DuckDBPhysicalNode")
            }
        }
    }
}

impl ExecutionPlan for DuckDBPhysicalNode {
    fn name(&self) -> &'static str {
        "DuckDBPhysicalNode"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            // Using DataFusionError::External is currently not possible as it requires Sync
            return Err(DataFusionError::Internal(
                error::LogicalExtensionChildCountSnafu {
                    name: "DuckDBPhysicalNode".to_string(),
                    expected: 0usize,
                }
                .build()
                .to_string(),
            ));
        }
        Ok(Arc::new(Self::new(self.query.clone(), self.schema.clone())))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion_physical_plan::SendableRecordBatchStream> {
        let duckdb_pool = Arc::new(
            DuckDbConnectionPool::new_memory()
                .context(ex_error::DuckdbConnectionPoolSnafu)?
                .with_connection_setup_queries(setup_queries),
        );

        let stream = get_stream(duckdb_pool, sql, Arc::new(ArrowSchema::empty()))
            .await
            .context(ex_error::DataFusionSnafu)?;
    }
}
