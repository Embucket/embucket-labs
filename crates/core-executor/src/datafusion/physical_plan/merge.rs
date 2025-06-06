use datafusion::arrow::{
    array::{Array, RecordBatch, StringArray, downcast_array},
    compute::{filter, kernels::cmp::distinct},
};
use datafusion_common::{DFSchemaRef, DataFusionError, HashSet};
use datafusion_iceberg::{DataFusionTable, error::Error as DataFusionIcebergError};
use datafusion_physical_plan::{
    DisplayAs, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::tabular::Tabular,
    error::Error as IcebergError,
};
use pin_project_lite::pin_project;
use std::{sync::Arc, task::Poll};

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
    fn name(&self) -> &'static str {
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

        let filtered = SourceExistFilterStream::new(input);

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

                let datafiles = write_parquet_partitioned(
                    table,
                    filtered.map_err(Into::into),
                    branch.as_deref(),
                )
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

// Filters a stream of Recordbatches by whether the value in the "__data_file_column" already had
// a row where the "__source_exists" column equals "true".
pin_project! {
    pub struct SourceExistFilterStream {
        // Files which already encountered a "__source_exists" = true value
        matching_files: HashSet<String>,
        // The current datafiles being processed
        current_file: Option<String>,
        // If the current datafiles hasn't had any rows with "__source_exists" = true, this is used
        // to buffer the recordbatches. If a "__source_exists" = true is encountered in a later
        // recordbatch, the buffered recordbatches will be returned. If the `current_file` is set
        // to another value, the buffer can be discarded as there won't be more any records from
        // the same file.
        current_buffer: Vec<RecordBatch>,
        // RecordBatches ready to be consumed
        ready_batches: Vec<RecordBatch>,

        #[pin]
        input: SendableRecordBatchStream
    }
}

impl SourceExistFilterStream {
    fn new(input: SendableRecordBatchStream) -> Self {
        Self {
            matching_files: HashSet::new(),
            current_file: None,
            current_buffer: Vec::new(),
            ready_batches: Vec::new(),
            input,
        }
    }
}

impl Stream for SourceExistFilterStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let project = self.project();

        // Return early if a batch is ready
        if let Some(batch) = project.ready_batches.pop() {
            return Poll::Ready(Some(Ok(batch)));
        }

        match project.input.poll_next(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(batch)),
            x => x,
        }
    }
}

impl RecordBatchStream for SourceExistFilterStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        todo!()
    }
}

// Computes a HashSet of all strings values in the array
fn unique_values(array: &dyn Array) -> Result<HashSet<String>, DataFusionError> {
    let slice_len = array.len() - 1;
    let v1 = array.slice(0, slice_len);
    let v2 = array.slice(1, slice_len);

    let mask = distinct(&v1, &v2)?;

    let unique = filter(&v2, &mask)?;

    let strings = downcast_array::<StringArray>(&unique);

    let first = strings.value(0).to_owned();

    let result = strings
        .iter()
        .fold(HashSet::from_iter([first]), |mut acc, x| {
            if let Some(x) = x {
                acc.insert(x.to_owned());
            }
            acc
        });

    Ok(result)
}
