use std::sync::Arc;

use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;

use crate::error;

#[derive(Debug, Clone)]
pub struct DuckDBPhysicalNode {
    query: String,
    schema: DFSchemaRef,
    setup_queries: Vec<Arc<str>>,
    properties: PlanProperties,
}

impl DuckDBPhysicalNode {
    pub fn new(query: String, schema: DFSchemaRef, setup_queries: Vec<Arc<str>>) -> Self {
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
            setup_queries,
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
        Ok(Arc::new(Self::new(
            self.query.clone(),
            self.schema.clone(),
            self.setup_queries.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let duckdb_pool = Arc::new(
            DuckDbConnectionPool::new_memory()
                .map_err(|e| {
                    DataFusionError::Internal(format!("DuckDB connection pool error: {e}"))
                })?
                .with_connection_setup_queries(self.setup_queries.clone()),
        );

        let query = self.query.clone();
        let schema = Some(Arc::new(self.schema.as_ref().clone().into()));

        let conn = duckdb_pool
            .connect_sync()
            .map_err(|e| DataFusionError::Internal(format!("DuckDB connect error: {e}")))?;

        if let Some(conn) = conn.as_sync() {
            conn.query_arrow(&query, &[], schema)
                .map_err(|e| DataFusionError::Internal(format!("DuckDB query error: {e}")))
        } else {
            Err(DataFusionError::Internal(
                "Expected synchronous DuckDB connection".to_string(),
            ))
        }
    }
}
