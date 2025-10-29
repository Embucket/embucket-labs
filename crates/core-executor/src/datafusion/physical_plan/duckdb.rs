use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use duckdb::Connection;
use snafu::ResultExt;
use crate::duckdb::functions::register_all_udfs;
use crate::duckdb::query::{apply_connection_setup_queries, query_duck_db_arrow};
use crate::error;

#[derive(Debug)]
pub struct DuckDBExec {
    sql: String,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    schema: DFSchemaRef,
}

impl DuckDBExec {
    pub fn new(sql: String, input: Arc<dyn ExecutionPlan>, schema: DFSchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::new((*schema.as_arrow()).clone()));
        let partitioning = Partitioning::UnknownPartitioning(1); // Single partition for sink operations
        let emission_type = EmissionType::Final; // Final emission after all processing is complete
        let boundedness = Boundedness::Bounded; // Bounded operation that completes

        let properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);
        Self {
            sql,
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for DuckDBExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DuckDBExec")
            }
        }
    }
}

impl ExecutionPlan for DuckDBExec {
    fn name(&self) -> &'static str {
        "DuckDBExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            // Using DataFusionError::External is currently not possible as it requires Sync
            return Err(DataFusionError::Internal(
                error::LogicalExtensionChildCountSnafu {
                    name: "DuckDBExec".to_string(),
                    expected: 1usize,
                }
                    .build()
                    .to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.sql.clone(),
            children[0].clone(),
            self.schema.clone(),
        )))
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let connection = Connection::open_in_memory().context(crate::error::DuckdbSnafu).map_err(|e| datafusion_common::DataFusionError::from(e))?;
        query_duck_db_arrow(&connection, &self.sql).map_err(Into::into)
    }
}