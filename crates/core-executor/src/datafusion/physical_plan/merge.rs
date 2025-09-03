use datafusion::{
    arrow::{
        array::{Array, BooleanArray, RecordBatch, StringArray, downcast_array},
        compute::{
            filter, filter_record_batch,
            kernels::cmp::{distinct, eq},
            or, or_kleene,
        },
        datatypes::Schema,
    },
    physical_expr::EquivalenceProperties,
};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_iceberg::{
    DataFusionTable, error::Error as DataFusionIcebergError, table::write_parquet_data_files,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    expressions::Column,
    projection::ProjectionExec,
    stream::RecordBatchStreamAdapter,
};
use futures::{Stream, StreamExt};
use iceberg_rust::{catalog::tabular::Tabular, error::Error as IcebergError};
use lru::LruCache;
use pin_project_lite::pin_project;
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::error;

pub(crate) static TARGET_EXISTS_COLUMN: &str = "__target_exists";
pub(crate) static SOURCE_EXISTS_COLUMN: &str = "__source_exists";
pub(crate) static DATA_FILE_PATH_COLUMN: &str = "__data_file_path";
pub(crate) static MANIFEST_FILE_PATH_COLUMN: &str = "__manifest_file_path";
static BUFFER_SIZE: usize = 2;

#[derive(Debug)]
pub struct MergeIntoCOWSinkExec {
    schema: DFSchemaRef,
    input: Arc<dyn ExecutionPlan>,
    target: DataFusionTable,
    properties: PlanProperties,
}

impl MergeIntoCOWSinkExec {
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

impl DisplayAs for MergeIntoCOWSinkExec {
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

// Map from Manifest file to contained Datafiles
type ManifestAndDataFiles = HashMap<String, Vec<String>>;

impl ExecutionPlan for MergeIntoCOWSinkExec {
    fn name(&self) -> &'static str {
        "MergeIntoCOWSinkExec"
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
            // Using DataFusionError::External is currently not possible as it requires Sync
            return Err(DataFusionError::Internal(
                error::LogicalExtensionChildCountSnafu {
                    name: "MergeIntoCOWSinkExec".to_string(),
                    expected: 1usize,
                }
                .build()
                .to_string(),
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

        let matching_files: Arc<Mutex<Option<ManifestAndDataFiles>>> = Arc::default();

        // Filter out rows whoose __data_file_path doesn't have a matching row
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(MergeCOWFilterExec::new(
            self.input.clone(),
            matching_files.clone(),
        ));

        // Remove auxiliary columns
        let projection =
            ProjectionExec::try_new(schema_projection(&self.input.schema()), filtered)?;

        let batches = projection.execute(partition, context.clone())?;

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
                let datafiles =
                    write_parquet_data_files(table, batches, &context, branch.as_deref()).await?;

                let matching_files = {
                    #[allow(clippy::unwrap_used)]
                    let mut lock = matching_files.lock().unwrap();
                    lock.take().ok_or_else(|| {
                        DataFusionError::Internal(
                            error::MatchingFilesAlreadyConsumedSnafu {}
                                .build()
                                .to_string(),
                        )
                    })?
                };

                if !datafiles.is_empty() {
                    // Commit transaction on Iceberg table
                    if matching_files.is_empty() {
                        table
                            .new_transaction(branch.as_deref())
                            .append_data(datafiles)
                            .commit()
                            .await
                            .context(error::IcebergSnafu)?;
                    } else {
                        table
                            .new_transaction(branch.as_deref())
                            .overwrite(datafiles, matching_files)
                            .commit()
                            .await
                            .context(error::IcebergSnafu)?;
                    }
                }

                Ok(RecordBatch::new_empty(schema))
            }
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct MergeCOWFilterExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    matching_files: Arc<Mutex<Option<ManifestAndDataFiles>>>,
}

impl MergeCOWFilterExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        matching_files: Arc<Mutex<Option<ManifestAndDataFiles>>>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            properties,
            matching_files,
        }
    }
}

impl DisplayAs for MergeCOWFilterExec {
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

impl ExecutionPlan for MergeCOWFilterExec {
    fn name(&self) -> &'static str {
        "MergeCOWFilterExec"
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
                error::LogicalExtensionChildCountSnafu {
                    name: "MergeCOWFilterExec".to_string(),
                    expected: 1usize,
                }
                .build()
                .to_string(),
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
        Ok(Box::pin(MergeCOWFilterStream::new(
            self.input.execute(partition, context)?,
            self.matching_files.clone(),
        )))
    }
}

pin_project! {
    /// A streaming filter for Copy-on-Write (COW) merge operations that tracks file matching state.
    ///
    /// This stream processes record batches and maintains state about which data files have
    /// matching rows (where `__source_exists` = true) and which do not. It buffers batches
    /// from non-matching files until their matching status is determined, then releases them
    /// when appropriate.
    ///
    /// The stream is used to efficiently handle merge operations by:
    /// - Tracking files that have already found matching rows
    /// - Buffering data from files that haven't found matches yet
    /// - Managing the flow of data to optimize merge performance
    pub struct MergeCOWFilterStream {
        // Files which already encountered a "__source_exists" = true value
        matching_files: HashMap<String,String>,
        // Reference to store the matching files after the stream has finished executing
        matching_files_ref: Arc<Mutex<Option<ManifestAndDataFiles>>>,
        // Files which haven't encountered a "__source_exists" = true value
        not_matching_files: HashMap<String,String>,
        // Buffer of RecordBatches whoose data files haven't had a matching row yet
        not_matched_buffer: LruCache<String, Vec<RecordBatch>>,
        // Previously buffered RecordBatches that are now ready to be consumed
        ready_batches: Vec<RecordBatch>,

        #[pin]
        input: SendableRecordBatchStream,
    }
}

impl MergeCOWFilterStream {
    fn new(
        input: SendableRecordBatchStream,
        matching_files_ref: Arc<Mutex<Option<ManifestAndDataFiles>>>,
    ) -> Self {
        Self {
            matching_files: HashMap::new(),
            not_matching_files: HashMap::new(),
            #[allow(clippy::unwrap_used)]
            not_matched_buffer: LruCache::new(NonZeroUsize::new(BUFFER_SIZE).unwrap()),
            ready_batches: Vec::new(),
            matching_files_ref,
            input,
        }
    }
}

impl Stream for MergeCOWFilterStream {
    type Item = Result<RecordBatch, DataFusionError>;

    #[allow(clippy::too_many_lines)]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut project = self.project();

        loop {
            // Return early if a batch is ready
            if let Some(batch) = project.ready_batches.pop() {
                return Poll::Ready(Some(Ok(batch)));
            }

            match project.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let schema = batch.schema();

                    let source_exists = batch.column(schema.index_of(SOURCE_EXISTS_COLUMN)?);
                    let data_file_path = batch.column(schema.index_of(DATA_FILE_PATH_COLUMN)?);
                    let manifest_file_path =
                        batch.column(schema.index_of(MANIFEST_FILE_PATH_COLUMN)?);

                    let filtered_data_file_path = filter(
                        &data_file_path,
                        &downcast_array::<BooleanArray>(source_exists),
                    )?;

                    let all_data_and_manifest_files =
                        unique_files_and_manifests(&data_file_path, &manifest_file_path)?;

                    let matching_data_files = create_matching_data_files(
                        project.matching_files,
                        &filtered_data_file_path,
                        &all_data_and_manifest_files,
                    )?;

                    let newly_matched_data_files: HashSet<String> = project
                        .not_matching_files
                        .keys()
                        .map(ToOwned::to_owned)
                        .collect::<HashSet<String>>()
                        .intersection(&matching_data_files)
                        .map(ToOwned::to_owned)
                        .collect();

                    let matching_data_and_manifest_files = collect_matching_data_and_manifest_files(
                        &batch,
                        all_data_and_manifest_files,
                        &matching_data_files,
                        project.not_matched_buffer,
                    );

                    if matching_data_and_manifest_files.is_empty() {
                        // If there are no rows where source_exists is true
                        if !filtered_data_file_path.is_empty() {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        continue;
                    }
                    // When datafile didn't match in previous record batches but matches now, the
                    // previous record batches have to be appended to the output
                    for file in newly_matched_data_files {
                        let manifest =
                            project.not_matching_files.remove(&file).ok_or_else(|| {
                                DataFusionError::Internal(
                                    error::MergeFilterStreamNotMatchingSnafu { file: file.clone() }
                                        .build()
                                        .to_string(),
                                )
                            })?;

                        let batches = project.not_matched_buffer.pop(&file).ok_or_else(|| {
                            DataFusionError::Internal(
                                error::MergeFilterStreamNotMatchingSnafu { file: file.clone() }
                                    .build()
                                    .to_string(),
                            )
                        })?;

                        for batch in batches {
                            let schema = batch.schema();
                            let data_file_path =
                                batch.column(schema.index_of(DATA_FILE_PATH_COLUMN)?);

                            let predicate = eq(&data_file_path, &StringArray::new_scalar(&file))?;
                            let filtered_batch = filter_record_batch(&batch, &predicate)?;

                            project.ready_batches.push(filtered_batch);
                        }

                        project.matching_files.insert(file, manifest);
                    }

                    let file_predicate = matching_data_files
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
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                error::MissingFilterPredicatesSnafu {}.build().to_string(),
                            )
                        })?;

                    let predicate = or_kleene(
                        &file_predicate,
                        &downcast_array::<BooleanArray>(&source_exists),
                    )?;

                    project
                        .matching_files
                        .extend(matching_data_and_manifest_files);

                    let filtered_batch = filter_record_batch(&batch, &predicate)?;

                    return Poll::Ready(Some(Ok(filtered_batch)));
                }
                Poll::Ready(None) => {
                    // The stream has finished, we now have to pass the list of matched files to the
                    // matching_files_ref to be accessed from outside of this stream
                    let mut matching_files = std::mem::take(project.matching_files);
                    let mut new: HashMap<String, Vec<String>> = HashMap::new();
                    for (file, manifest) in matching_files.drain() {
                        new.entry(manifest)
                            .and_modify(|v| v.push(file.clone()))
                            .or_insert_with(|| vec![file]);
                    }
                    #[allow(clippy::unwrap_used)]
                    let mut lock = project.matching_files_ref.lock().unwrap();
                    lock.replace(new);
                    return Poll::Ready(None);
                }
                x => return x,
            }
        }
    }
}

impl RecordBatchStream for MergeCOWFilterStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

/// Separates data files into matching and non-matching categories.
///
/// For each data file in `all_data_and_manifest_files`:
/// - If the file is in `matching_data_files`, it's added to the returned map
/// - If the file is not matching, the current batch is buffered in `not_matched_buffer`
///   for potential future processing if the file becomes matching later
///
/// # Arguments
/// * `batch` - The current record batch to buffer for non-matching files
/// * `all_data_and_manifest_files` - Map of data file paths to their manifest file paths
/// * `matching_data_files` - Set of data file paths that have matching conditions
/// * `not_matched_buffer` - LRU cache to buffer batches for non-matching files
///
/// # Returns
/// A map containing only the data files that match, paired with their manifest files
fn collect_matching_data_and_manifest_files(
    batch: &RecordBatch,
    mut all_data_and_manifest_files: HashMap<String, String>,
    matching_data_files: &HashSet<String>,
    not_matched_buffer: &mut LruCache<String, Vec<RecordBatch>>,
) -> HashMap<String, String> {
    let mut matching_data_and_manifest_files: HashMap<String, String> = HashMap::new();

    for (file, manifest) in all_data_and_manifest_files.drain() {
        if matching_data_files.contains(&file) {
            matching_data_and_manifest_files.insert(file, manifest);
        } else if let Some(batches) = not_matched_buffer.get_mut(&file) {
            batches.push(batch.clone());
        } else {
            not_matched_buffer.put(file, vec![batch.clone()]);
        }
    }
    matching_data_and_manifest_files
}

/// Creates a set of data files that need to be processed based on matching criteria.
///
/// This function combines:
/// 1. Files from the current batch that have `__source_exists` = true (`filtered_data_file_path`)
/// 2. Files that were previously identified as matching and are present in the current batch
///
/// The result represents all data files that require Copy-on-Write rewriting because they
/// contain rows that match the merge condition.
///
/// # Arguments
/// * `project_matching_files` - Previously identified matching files from earlier batches
/// * `filtered_data_file_path` - Array of data file paths where `__source_exists` is true
/// * `all_data_and_manifest_files` - All data files present in the current batch
///
/// # Returns
/// A set of data file paths that need to be processed for the merge operation
fn create_matching_data_files(
    project_matching_files: &HashMap<String, String>,
    filtered_data_file_path: &dyn Array,
    all_data_and_manifest_files: &HashMap<String, String>,
) -> Result<HashSet<String>, DataFusionError> {
    let mut matching_data_files = unique_values(filtered_data_file_path)?;

    let all_data_files: HashSet<String> = all_data_and_manifest_files
        .keys()
        .map(ToOwned::to_owned)
        .collect();

    let previously_matched_data_files: HashSet<String> = project_matching_files
        .keys()
        .map(ToOwned::to_owned)
        .collect::<HashSet<String>>()
        .intersection(&all_data_files)
        .map(ToOwned::to_owned)
        .collect();

    matching_data_files.extend(previously_matched_data_files);
    Ok(matching_data_files)
}

/// Extracts unique string values from an array efficiently by only comparing consecutive elements.
///
/// This function assumes the input array is sorted and leverages this property to find unique values
/// by only checking consecutive pairs rather than comparing all elements. It:
/// 1. Takes the first element as a starting point
/// 2. Identifies positions where consecutive elements differ
/// 3. Filters to keep only the distinct values
/// 4. Returns a `HashSet` containing all unique string values
///
/// # Arguments
/// * `array` - A reference to an Array (expected to be a `StringArray`)
///
/// # Returns
/// * `Result<HashSet<String>, DataFusionError>` - `HashSet` of unique string values or an error
fn unique_values(array: &dyn Array) -> Result<HashSet<String>, DataFusionError> {
    if array.is_empty() {
        return Ok(HashSet::new());
    }

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

    let init = if first.is_empty() {
        HashSet::new()
    } else {
        HashSet::from_iter([first])
    };

    let result = strings.iter().fold(init, |mut acc, x| {
        if let Some(x) = x
            && !x.is_empty()
        {
            acc.insert(x.to_owned());
        }
        acc
    });

    Ok(result)
}

/// Creates a mapping of unique file paths to their corresponding manifest paths.
///
/// This function efficiently extracts unique file-manifest pairs from two sorted arrays by
/// comparing consecutive elements. It assumes both arrays are sorted and of equal length.
/// The function:
/// 1. Takes the first file-manifest pair as a starting point
/// 2. Identifies positions where consecutive file entries differ
/// 3. Filters both arrays to keep only the distinct file-manifest pairs
/// 4. Returns a `HashMap` mapping file paths to manifest paths
///
/// # Arguments
/// * `files` - A reference to an Array containing file paths (expected to be a `StringArray`)
/// * `manifests` - A reference to an Array containing manifest paths (expected to be a `StringArray`)
///
/// # Returns
/// * `Result<HashMap<String, String>, DataFusionError>` - `HashMap` mapping file paths to manifest paths or an error
fn unique_files_and_manifests(
    files: &dyn Array,
    manifests: &dyn Array,
) -> Result<HashMap<String, String>, DataFusionError> {
    if files.is_empty() {
        return Ok(HashMap::new());
    }

    let first_file = downcast_array::<StringArray>(files).value(0).to_owned();
    let first_manifest = downcast_array::<StringArray>(manifests).value(0).to_owned();

    let slice_len = files.len() - 1;

    if slice_len == 0 {
        return Ok(HashMap::from_iter([(first_file, first_manifest)]));
    }

    let v1 = files.slice(0, slice_len);
    let v2 = files.slice(1, slice_len);

    let manifests = manifests.slice(1, slice_len);

    // Which consecutive entries are different
    let mask = distinct(&v1, &v2)?;

    // only keep values that are diffirent from their previous value, this drastically reduces the
    // number of values needed to process
    let unique_files = filter(&v2, &mask)?;

    let unique_manifests = filter(&manifests, &mask)?;

    let file_strings = downcast_array::<StringArray>(&unique_files);
    let manifest_strings = downcast_array::<StringArray>(&unique_manifests);

    let init = if first_file.is_empty() {
        HashMap::new()
    } else {
        HashMap::from_iter([(first_file, first_manifest)])
    };

    let result =
        manifest_strings
            .iter()
            .zip(file_strings.iter())
            .fold(init, |mut acc, (manifest, file)| {
                if let (Some(manifest), Some(file)) = (manifest, file)
                    && !file.is_empty()
                {
                    acc.insert(file.to_owned(), manifest.to_owned());
                }
                acc
            });

    Ok(result)
}

/// Creates a projection expression list from a schema by filtering out auxiliary columns.
///
/// This function builds a vector of physical expressions and column names from the given schema,
/// excluding internal auxiliary columns used for merge operations. The auxiliary columns that
/// are filtered out are:
/// - `__source_exists`: Indicates if the source record exists
/// - `__data_file_path`: Path to the data file  
/// - `__manifest_file_path`: Path to the manifest file
///
/// # Arguments
/// * `schema` - The schema to create projections from
///
/// # Returns
/// * `Vec<(Arc<dyn PhysicalExpr>, String)>` - Vector of tuples containing physical expressions and column names
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
    use datafusion::arrow::array::{GenericStringBuilder, Int32Array};
    use datafusion::arrow::compute;
    use datafusion::arrow::datatypes::{DataType, Field};

    use super::*;
    use std::sync::Arc;

    macro_rules! test_source_exist_filter_stream {
        ($test_name:ident, $input_slice:expr, $expected_sum:expr) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_source_exist_filter_stream_ $test_name>]() {
                    use datafusion::arrow::datatypes::{DataType, Field};
                    use futures::stream;

                    let schema = Arc::new(Schema::new(vec![
                        Field::new(SOURCE_EXISTS_COLUMN, DataType::Boolean, true),
                        Field::new(DATA_FILE_PATH_COLUMN, DataType::Utf8, true),
                        Field::new(MANIFEST_FILE_PATH_COLUMN, DataType::Utf8, true),
                        Field::new("data", DataType::Int32, false),
                    ]));

                    let input_stream = stream::iter(build_input_stream($input_slice));

                    let stream = Box::pin(RecordBatchStreamAdapter::new(schema, input_stream));

                    let matching_files = Arc::default();

                    let mut filter_stream = MergeCOWFilterStream::new(stream, matching_files);

                    let mut sum = 0;
                    while let Some(result) = StreamExt::next(&mut filter_stream).await {
                        let batch = result.unwrap();
                        let data = batch.column(3);

                        sum += compute::sum(&downcast_array::<Int32Array>(&data)).unwrap();
                    }

                    assert_eq!(sum, $expected_sum);
                }
            }
        };
    }

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
    async fn test_source_exist_filter_stream_simple() {
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

        let mut filter_stream = MergeCOWFilterStream::new(stream, matching_files);

        let mut total_rows = 0;
        while let Some(result) = StreamExt::next(&mut filter_stream).await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
        }

        assert!(total_rows == 9);
    }

    fn build_record_batch(
        input: &[(
            Vec<bool>,
            Vec<Option<String>>,
            Vec<Option<String>>,
            Vec<i32>,
        )],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SOURCE_EXISTS_COLUMN, DataType::Boolean, true),
            Field::new(DATA_FILE_PATH_COLUMN, DataType::Utf8, true),
            Field::new(MANIFEST_FILE_PATH_COLUMN, DataType::Utf8, true),
            Field::new("data", DataType::Int32, false),
        ]));

        let mut b_builder = BooleanArray::builder(8);
        let mut df_builder = GenericStringBuilder::<i32>::new();
        let mut mf_builder = GenericStringBuilder::<i32>::new();
        let mut d_builder = Int32Array::builder(8);
        for (b, df, mf, d) in input {
            b_builder.append_slice(b);
            df_builder.append_array(&StringArray::from(df.clone()));
            mf_builder.append_array(&StringArray::from(mf.clone()));
            d_builder.append_slice(d);
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(b_builder.finish()),
                Arc::new(df_builder.finish()),
                Arc::new(mf_builder.finish()),
                Arc::new(d_builder.finish()),
            ],
        )
        .expect("Failed to build record batch")
    }

    fn generate_target(
        i: i32,
    ) -> (
        Vec<bool>,
        Vec<Option<String>>,
        Vec<Option<String>>,
        Vec<i32>,
    ) {
        (
            vec![false, false, false, false],
            vec![
                Some(format!("file${i}")),
                Some(format!("file${i}")),
                Some(format!("file${i}")),
                Some(format!("file${i}")),
            ],
            vec![
                Some(format!("manifest${i}")),
                Some(format!("manifest${i}")),
                Some(format!("manifest${i}")),
                Some(format!("manifest${i}")),
            ],
            vec![i * 4 + 1, i * 4 + 2, i * 4 + 3, i * 4 + 4],
        )
    }

    fn generate_source(
        i: i32,
    ) -> (
        Vec<bool>,
        Vec<Option<String>>,
        Vec<Option<String>>,
        Vec<i32>,
    ) {
        (
            vec![true, true, true, true],
            vec![None, None, None, None],
            vec![None, None, None, None],
            vec![i * 4 + 1, i * 4 + 2, i * 4 + 3, i * 4 + 4],
        )
    }

    fn generate_matching(
        i: i32,
    ) -> (
        Vec<bool>,
        Vec<Option<String>>,
        Vec<Option<String>>,
        Vec<i32>,
    ) {
        (
            vec![true, true, true, true],
            vec![
                Some(format!("file${i}")),
                Some(format!("file${i}")),
                Some(format!("file${i}")),
                Some(format!("file${i}")),
            ],
            vec![
                Some(format!("manifest${i}")),
                Some(format!("manifest${i}")),
                Some(format!("manifest${i}")),
                Some(format!("manifest${i}")),
            ],
            vec![i * 4 + 1, i * 4 + 2, i * 4 + 3, i * 4 + 4],
        )
    }

    fn build_input_stream(
        sequence: &[(usize, usize)],
    ) -> Vec<Result<RecordBatch, DataFusionError>> {
        sequence
            .iter()
            .map(|(n, i)| {
                let n: i32 = (*n).try_into().unwrap();
                match i {
                    1 => Ok(build_record_batch(&[generate_target(n)])),
                    2 => Ok(build_record_batch(&[generate_source(n)])),
                    3 => Ok(build_record_batch(&[
                        generate_target(n),
                        generate_source(n),
                    ])),
                    4 => Ok(build_record_batch(&[generate_matching(n)])),
                    5 => Ok(build_record_batch(&[
                        generate_target(n),
                        generate_matching(n),
                    ])),
                    6 => Ok(build_record_batch(&[
                        generate_source(n),
                        generate_matching(n),
                    ])),
                    7 => Ok(build_record_batch(&[
                        generate_source(n),
                        generate_target(n),
                        generate_matching(n),
                    ])),

                    _ => panic!(),
                }
            })
            .collect()
    }

    #[test]
    fn test_unique_files_and_manifests_with_duplicates() {
        let files = Arc::new(StringArray::from(vec![
            "file1", "file2", "file3", "file4", "file5",
        ]));
        let manifests = Arc::new(StringArray::from(vec![
            "manifest1",
            "manifest1",
            "manifest2",
            "manifest2",
            "manifest3",
        ]));

        let result = unique_files_and_manifests(files.as_ref(), manifests.as_ref()).unwrap();

        let expected = HashMap::from_iter([
            ("file1".to_string(), "manifest1".to_string()),
            ("file2".to_string(), "manifest1".to_string()),
            ("file3".to_string(), "manifest2".to_string()),
            ("file4".to_string(), "manifest2".to_string()),
            ("file5".to_string(), "manifest3".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    test_source_exist_filter_stream!(single_target, &[(1, 1)], 0);
    test_source_exist_filter_stream!(single_source, &[(1, 2)], 10);
    test_source_exist_filter_stream!(single_matching, &[(1, 4)], 10);
}
