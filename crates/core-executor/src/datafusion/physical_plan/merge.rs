use datafusion::{
    arrow::{
        array::{Array, BooleanArray, RecordBatch, StringArray, downcast_array},
        compute::{
            filter, filter_record_batch,
            kernels::cmp::{distinct, eq},
            or,
        },
        datatypes::Schema,
    },
    physical_expr::EquivalenceProperties,
};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_iceberg::{DataFusionTable, error::Error as DataFusionIcebergError};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    expressions::Column,
    projection::ProjectionExec,
    stream::RecordBatchStreamAdapter,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::tabular::Tabular,
    error::Error as IcebergError,
};
use pin_project_lite::pin_project;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    task::Poll,
};

static SOURCE_EXISTS_COLUMN: &str = "__source_exists";
pub(crate) static DATA_FILE_PATH_COLUMN: &str = "__data_file_path";
pub(crate) static MANIFEST_FILE_PATH_COLUMN: &str = "__manifest_file_path";

#[derive(Debug)]
pub struct MergeIntoSinkExec {
    schema: DFSchemaRef,
    input: Arc<dyn ExecutionPlan>,
    target: DataFusionTable,
    properties: PlanProperties,
}

impl MergeIntoSinkExec {
    pub fn new(
        schema: DFSchemaRef,
        input: Arc<dyn ExecutionPlan>,
        target: DataFusionTable,
    ) -> Self {
        // MERGE operations produce a single empty record batch after completion
        let eq_properties = EquivalenceProperties::new(Arc::new((*schema.as_arrow()).clone()));
        let partitioning = Partitioning::UnknownPartitioning(1); // Single partition for sink operations
        let emission_type = EmissionType::Final; // Final emission after all processing is complete
        let boundedness = Boundedness::Bounded; // Bounded operation that completes

        let properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);
        Self {
            schema,
            input,
            target,
            properties,
        }
    }
}

impl DisplayAs for MergeIntoSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MergeIntoSinkExec")
            }
        }
    }
}

impl ExecutionPlan for MergeIntoSinkExec {
    fn name(&self) -> &'static str {
        "MergeIntoSinkExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "MergeIntoSinkExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.schema.clone(),
            children[0].clone(),
            self.target.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion_physical_plan::SendableRecordBatchStream> {
        let schema = Arc::new(self.schema.as_arrow().clone());

        let matching_files: Arc<Mutex<Option<HashMap<String, Vec<String>>>>> = Arc::default();

        // Filter out rows whoose __data_file_path doesn't have a matching row
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(SourceExistFilterExec::new(
            self.input.clone(),
            matching_files.clone(),
        ));

        // Remove auxiliary columns
        let projection =
            ProjectionExec::try_new(schema_projection(&self.input.schema()), filtered)?;

        let batches = projection.execute(partition, context)?;

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

                // Write recordbatches into parquet files on object-storage
                let datafiles = write_parquet_partitioned(
                    table,
                    batches.map_err(Into::into),
                    branch.as_deref(),
                )
                .await?;

                let matching_files = {
                    #[allow(clippy::unwrap_used)]
                    let mut lock = matching_files.lock().unwrap();
                    lock.take().ok_or_else(|| {
                        DataFusionError::Internal(
                            "Matching files have already been consumed".to_string(),
                        )
                    })?
                };

                // Commit transaction on Iceberg table
                table
                    .new_transaction(branch.as_deref())
                    .overwrite(datafiles, matching_files)
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

#[derive(Debug)]
struct SourceExistFilterExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    matching_files: Arc<Mutex<Option<HashMap<String, Vec<String>>>>>,
}

impl SourceExistFilterExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        matching_files: Arc<Mutex<Option<HashMap<String, Vec<String>>>>>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            properties,
            matching_files,
        }
    }
}

impl DisplayAs for SourceExistFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "SourceExistFilterExec")
            }
        }
    }
}

impl ExecutionPlan for SourceExistFilterExec {
    fn name(&self) -> &'static str {
        "SourceExistFilterExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SourceExistFilterExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.matching_files.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(SourceExistFilterStream::new(
            self.input.execute(partition, context)?,
            self.matching_files.clone(),
        )))
    }
}

// Filters a stream of Recordbatches by whether the value in the "__data_file_column" already had
// a row where the "__source_exists" column equals "true".
pin_project! {
    pub struct SourceExistFilterStream {
        // Files which already encountered a "__source_exists" = true value
        matching_files: HashMap<String,String>,
        // Reference to store the mathcing files after the stream has finished executing
        matching_files_ref: Arc<Mutex<Option<HashMap<String,Vec<String>>>>>,
        // The current datafiles being processed
        last_files: Option<String>,
        // If the current datafiles hasn't had any rows with "__source_exists" = true, this is used
        // to buffer the recordbatches. If a "__source_exists" = true is encountered in a later
        // recordbatch, the buffered recordbatches will be returned. If the `current_file` is set
        // to another value, the buffer can be discarded as there won't be more any records from
        // the same file.
        current_buffers: HashMap<String, RecordBatch>,
        // RecordBatches ready to be consumed
        ready_batches: Vec<RecordBatch>,

        #[pin]
        input: SendableRecordBatchStream,
    }
}

impl SourceExistFilterStream {
    fn new(
        input: SendableRecordBatchStream,
        matching_files_ref: Arc<Mutex<Option<HashMap<String, Vec<String>>>>>,
    ) -> Self {
        Self {
            matching_files: HashMap::new(),
            last_files: None,
            current_buffers: HashMap::new(),
            ready_batches: Vec::new(),
            matching_files_ref,
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
            Poll::Ready(Some(Ok(batch))) => {
                let schema = batch.schema();

                let source_exists_index = schema.index_of(SOURCE_EXISTS_COLUMN)?;
                let data_file_path_index = schema.index_of(DATA_FILE_PATH_COLUMN)?;
                let manifest_file_path_index = schema.index_of(MANIFEST_FILE_PATH_COLUMN)?;

                let source_exists = batch.column(source_exists_index);
                let data_file_path = batch.column(data_file_path_index);
                let manifest_file_path = batch.column(manifest_file_path_index);

                let filtered_data_file_path = filter(
                    &data_file_path,
                    &downcast_array::<BooleanArray>(source_exists),
                )?;

                let mut all_data_and_manifest_files =
                    unique_files_and_manifests(&data_file_path, &manifest_file_path)?;

                let mut matching_data_files = unique_values(&filtered_data_file_path)?;

                let all_data_files: HashSet<String> =
                    HashSet::from_iter(all_data_and_manifest_files.keys().map(ToOwned::to_owned));

                let already_matched_data_files: HashSet<String> =
                    HashSet::from_iter(project.matching_files.keys().map(ToOwned::to_owned))
                        .intersection(&all_data_files)
                        .map(ToOwned::to_owned)
                        .collect();

                matching_data_files.extend(already_matched_data_files);

                let mut matching_data_and_manifest_files: HashMap<String, String> = HashMap::new();
                let mut not_matching_data_and_manifest_files: HashMap<String, String> =
                    HashMap::new();

                for (file, manifest) in all_data_and_manifest_files.drain() {
                    if matching_data_files.contains(&file) {
                        matching_data_and_manifest_files.insert(file, manifest);
                    } else {
                        not_matching_data_and_manifest_files.insert(file, manifest);
                    }
                }

                if matching_data_and_manifest_files.is_empty() {
                    Poll::Pending
                } else {
                    let predicate = matching_data_files
                        .iter()
                        .try_fold(None::<BooleanArray>, |acc, x| {
                            let new = eq(&data_file_path, &StringArray::new_scalar(x))?;
                            if let Some(acc) = acc {
                                let result = or(&acc, &new)?;
                                Ok::<_, DataFusionError>(Some(result))
                            } else {
                                Ok(Some(new))
                            }
                        })?
                        .expect("Matching files cannot be empty");

                    project
                        .matching_files
                        .extend(matching_data_and_manifest_files);

                    let filtered_batch = filter_record_batch(&batch, &predicate)?;

                    Poll::Ready(Some(Ok(filtered_batch)))
                }
            }
            Poll::Ready(None) => {
                let mut matching_files = std::mem::take(project.matching_files);
                let mut new: HashMap<String, Vec<String>> = HashMap::new();
                for (file, manifest) in matching_files.drain() {
                    new.entry(manifest)
                        .and_modify(|v| v.push(file.clone()))
                        .or_insert(vec![file]);
                }
                let mut lock = project
                    .matching_files_ref
                    .lock()
                    .expect("Failed to get lock on the matching files hashmap.");
                lock.replace(new);
                Poll::Ready(None)
            }
            x => x,
        }
    }
}

impl RecordBatchStream for SourceExistFilterStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

// Computes a HashSet of all string values in the array
fn unique_values(array: &dyn Array) -> Result<HashSet<String>, DataFusionError> {
    let first = downcast_array::<StringArray>(array).value(0).to_owned();

    let slice_len = array.len() - 1;

    if slice_len == 0 {
        return Ok(HashSet::from_iter([first]));
    }

    let v1 = array.slice(0, slice_len);
    let v2 = array.slice(1, slice_len);

    // Which consecutive entries are different
    let mask = distinct(&v1, &v2)?;

    // only keep values that are diffirent from their previous value, this drastically reduces the
    // number of values needed to process
    let unique = filter(&v2, &mask)?;

    let strings = downcast_array::<StringArray>(&unique);

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

// Computes a HashSet of all string values in the array
fn unique_files_and_manifests(
    files: &dyn Array,
    manifests: &dyn Array,
) -> Result<HashMap<String, String>, DataFusionError> {
    let first_file = downcast_array::<StringArray>(files).value(0).to_owned();
    let first_manifest = downcast_array::<StringArray>(manifests).value(0).to_owned();

    let slice_len = files.len() - 1;

    if slice_len == 0 {
        return Ok(HashMap::from_iter([(first_file, first_manifest)]));
    }

    let v1 = files.slice(0, slice_len);
    let v2 = files.slice(1, slice_len);

    let manifests = files.slice(1, slice_len);

    // Which consecutive entries are different
    let mask = distinct(&v1, &v2)?;

    // only keep values that are diffirent from their previous value, this drastically reduces the
    // number of values needed to process
    let unique_files = filter(&v2, &mask)?;

    let unique_manifests = filter(&manifests, &mask)?;

    let file_strings = downcast_array::<StringArray>(&unique_files);
    let manifest_strings = downcast_array::<StringArray>(&unique_manifests);

    let result = manifest_strings.iter().zip(file_strings.iter()).fold(
        HashMap::from_iter([(first_file, first_manifest)]),
        |mut acc, (manifest, file)| {
            if let (Some(manifest), Some(file)) = (manifest, file) {
                acc.insert(file.to_owned(), manifest.to_owned());
            }
            acc
        },
    );

    Ok(result)
}

// Remove auxiliary columns from schema
fn schema_projection(schema: &Schema) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, field)| -> Option<(Arc<dyn PhysicalExpr>, String)> {
            let name = field.name();
            if name != SOURCE_EXISTS_COLUMN
                && name != DATA_FILE_PATH_COLUMN
                && name != MANIFEST_FILE_PATH_COLUMN
            {
                Some((Arc::new(Column::new(name, i)), name.to_owned()))
            } else {
                None
            }
        })
        .collect()
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_unique_values_with_duplicates() {
        let array = Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"]));
        let result = unique_values(array.as_ref()).unwrap();

        let expected: HashSet<String> = ["a", "b", "c"].iter().map(|&s| s.to_string()).collect();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_unique_values_all_same() {
        let array = Arc::new(StringArray::from(vec!["same", "same", "same"]));
        let result = unique_values(array.as_ref()).unwrap();

        let expected: HashSet<String> = std::iter::once(&"same")
            .map(std::string::ToString::to_string)
            .collect();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_unique_values_with_nulls() {
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            None,
            Some("a"),
        ]));
        let result = unique_values(array.as_ref()).unwrap();

        let expected: HashSet<String> = ["a", "b"].iter().map(|&s| s.to_string()).collect();

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_source_exist_filter_stream() {
        use datafusion::arrow::datatypes::{DataType, Field};
        use futures::stream;

        let schema = Arc::new(Schema::new(vec![
            Field::new(SOURCE_EXISTS_COLUMN, DataType::Boolean, false),
            Field::new(DATA_FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(MANIFEST_FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new("data", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![false, false, true, false])),
                Arc::new(StringArray::from(vec!["file1", "file1", "file2", "file2"])),
                Arc::new(StringArray::from(vec![
                    "manifest1",
                    "manifest1",
                    "manifest1",
                    "manifest1",
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![false, true, false, true])),
                Arc::new(StringArray::from(vec!["file2", "file3", "file3", "file3"])),
                Arc::new(StringArray::from(vec![
                    "manifest1",
                    "manifest2",
                    "manifest2",
                    "manifest2",
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![5, 6, 7, 8])),
            ],
        )
        .unwrap();

        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true, true, false])),
                Arc::new(StringArray::from(vec!["file4", "file4", "file4"])),
                Arc::new(StringArray::from(vec![
                    "manifest3",
                    "manifest3",
                    "manifest3",
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![9, 10, 11])),
            ],
        )
        .unwrap();

        let input_stream = stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]);
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, input_stream));

        let matching_files = Arc::default();

        let mut filter_stream = SourceExistFilterStream::new(stream, matching_files);

        let mut total_rows = 0;
        while let Some(result) = StreamExt::next(&mut filter_stream).await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
        }

        assert!(total_rows == 9);
    }
}
