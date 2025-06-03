use datafusion::arrow::array::RecordBatch;
use datafusion_common::DFSchemaRef;
use datafusion_iceberg::{DataFusionTable, error::Error as DataFusionIcebergError};
use datafusion_physical_plan::{DisplayAs, ExecutionPlan, stream::RecordBatchStreamAdapter};
use futures::{StreamExt, TryStreamExt};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::tabular::Tabular,
    error::Error as IcebergError,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct MergeIntoSinkExec {
    schema: DFSchemaRef,
    input: Arc<dyn ExecutionPlan>,
    target: DataFusionTable,
}

impl MergeIntoSinkExec {
    pub fn new(
        schema: DFSchemaRef,
        input: Arc<dyn ExecutionPlan>,
        target: DataFusionTable,
    ) -> Self {
        Self {
            schema,
            input,
            target,
        }
    }
}

impl DisplayAs for MergeIntoSinkExec {
    fn fmt_as(
        &self,
        t: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for MergeIntoSinkExec {
    fn name(&self) -> &str {
        "MergeIntoSinkExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion_physical_plan::SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;

        let schema = Arc::new(self.schema.as_arrow().clone());

        //TODO
        // filter out files that don't need to be overwritten

        //TODO
        //Project input stream onto desired Schema

        let stream = futures::stream::once({
            let tabular = self.target.tabular.clone();
            let branch = self.target.branch.clone();
            let schema = schema.clone();
            async move {
                let mut lock = tabular.write().await;
                let table = if let Tabular::Table(table) = &mut *lock {
                    Ok(table)
                } else {
                    Err(IcebergError::InvalidFormat("database entity".to_string()))
                }
                .map_err(DataFusionIcebergError::from)?;

                let datafiles =
                    write_parquet_partitioned(table, input.map_err(Into::into), branch.as_deref())
                        .await?;

                table
                    .new_transaction(branch.as_deref())
                    .append_data(datafiles)
                    .commit()
                    .await
                    .map_err(DataFusionIcebergError::from)?;

                //TODO
                // remove files to be overwritten from iceberg metadata

                Ok(RecordBatch::new_empty(schema))
            }
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
