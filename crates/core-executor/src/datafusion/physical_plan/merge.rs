use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion_common::Column;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::internal_err;
use datafusion_expr::Case;
use datafusion_expr::Expr;
use datafusion_expr::JoinType;
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::RecordBatchStream;
use datafusion_physical_plan::joins::PartitionMode;
use datafusion_physical_plan::{ExecutionPlan, PlanProperties, SendableRecordBatchStream};
use futures::Stream;
use futures::StreamExt;

pub struct MergeExec {
    /// Target table scan plan
    pub target_plan: Arc<dyn ExecutionPlan>,
    /// Source table scan plan
    pub source_plan: Arc<dyn ExecutionPlan>,

    /// Left‐side (target) join columns
    pub left_join_keys: Vec<String>,
    /// Right‐side (source) join columns
    pub right_join_keys: Vec<String>,

    /// Join type for Merge: we want every source row, with matching target if it exists.
    pub join_type: JoinType,

    /// The schema of the joined plan (target ∪ source fields). You can build this
    /// ahead of time when you constructed `MergeExec`.
    pub join_schema: SchemaRef,

    /// The name of the “primary‐key” column on the target side (fully qualified if needed).
    pub target_pk_column: String,

    /// final output schema = (all join_schema fields) plus an extra `op` field.
    pub output_schema: SchemaRef,
}

/// Execution Plan for executing `Merge Statement`
///
/// Logic for merge is as follows:
///  1. Target table scan + source table scan executes batches to use for the
///     `JoinExec`
///  2. The join is executed to create the join batches which is wrapped in the
///     `Stream` helper.
///  3. The case expressions are evaluated against the join batches and based on
///     the evaluation it is assigned a '0' for `Match on Target` and a '1' for
///     `Not Match on Target`.
///  4. Batch inserts + updates based on column matching.
///     
/// ```text  
///  ┌──────────────────────┐                ┌──────────────────────┐
///  │                      │                │                      │
///  │     Target Scan      │                │     Source Scan      │
///  │                      │                │                      │
///  └──────────────────────┘                └──────────────────────┘
///              └───────────┐             ┌─────────────┘           
///                          ▼             ▼                         
///                      ┌──────────────────────┐                    
///                      │                      │                    
///                      │  Join Plan (+ Case   │                    
///                      │     Expressions)     │                    
///                      │                      │                    
///                      └──────────────────────┘                    
///                                  │                               
///                                  ▼                               
///                      ┌──────────────────────┐                    
///                      │                      │                    
///                      │  Add Column to Join  │                    
///                      │       Batches        │                    
///                      │                      │                    
///                      └──────────────────────┘   
///                                  │                               
///                                  ▼
///                      ┌──────────────────────┐                    
///                      │                      │                    
///                      │  StreamBatch Helper  │                    
///                      │                      │                    
///                      └──────────────────────┘    
///                                       
///              ┌───────────────────┴───────────────────┐           
///              ▼                                       ▼           
///  ┌──────────────────────┐                ┌──────────────────────┐
///  │                      │                │                      │
///  │    Batch Inserts     │                │    Batch Updates     │
///  │                      │                │                      │
///  └──────────────────────┘                └──────────────────────┘
/// ```
///
/// Currently only `Matched On Target` + `Not Matched On Target` is
/// supported as per Snowflake Specification
impl ExecutionPlan for MergeExec {
    fn name(&self) -> &'static str {
        "MergeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.target_plan, &self.source_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(DataFusionError::Internal(
                "MergeExec expects exactly 2 children".to_string(),
            ));
        }
        let new = MergeExec {
            target_plan: children[0].clone(),
            source_plan: children[1].clone(),
            left_join_keys: self.left_join_keys.clone(),
            right_join_keys: self.right_join_keys.clone(),
            join_type: self.join_type,
            join_schema: self.join_schema.clone(),
            target_pk_column: self.target_pk_column.clone(),
            output_schema: self.output_schema.clone(),
        };
        Ok(Arc::new(new))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    // TODO: Delete support (`2` in matching column)
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join_exec = JoinExec::try_new(
            self.target_plan.clone(),
            self.source_plan.clone(),
            self.left_join_keys
                .iter()
                .map(|k| Column::from(k.as_str()))
                .collect::<Vec<_>>(),
            self.right_join_keys
                .iter()
                .map(|k| Column::from(k.as_str()))
                .collect::<Vec<_>>(),
            self.join_type,
            None,
            PartitionMode::Partitioned,
            self.join_schema.clone(),
        )?;

        let joined_stream: SendableRecordBatchStream =
            join_exec.execute(partition, context.clone())?;

        // Locate pk_column index position
        let pk_index: usize = self
            .join_schema
            .fields()
            .iter()
            .position(|f| f.name() == &self.target_pk_column)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "MergeExec: cannot find target PK column “{}” in join schema",
                    self.target_pk_column
                ))
            })?;

        // Wrap the streaminto a custom `MergeStream` that, for each RecordBatch:
        //  1. computes is_null(joined_batch.column(pk_index))
        //  2. if is_null == true, set op = 1 (INSERT); else op = 0 (UPDATE)
        //  3. appends that op‑column as Int32Array to the RecordBatch
        //    Finally, we return a RecordBatchStream whose schema = (join_schema ∪ op:int32).
        let merge_stream = MergeStream::new(
            joined_stream,
            pk_index,
            self.join_schema.clone(),
            self.output_schema.clone(),
        );

        Ok(Box::pin(merge_stream))
    }
}

/// A helper stream that wraps a `RecordBatchStream`` of joined batches and
/// appends a new int32 column called “op” = 0/1.
struct MergeStream {
    /// JoinExec batches
    input: SendableRecordBatchStream,
    /// Column index of th e targeted row
    pk_index: usize,
    /// Schema of the joined batches
    join_schema: SchemaRef,
    /// output schema
    output_schema: SchemaRef,
}

impl MergeStream {
    fn new(
        input: SendableRecordBatchStream,
        pk_index: usize,
        join_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            pk_index,
            join_schema,
            output_schema,
        }
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for MergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // pulls the next joined batch from the upstream
        match ready!(this.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // column index holding the type of match
                let pk_array = batch.column(this.pk_index);

                // Build a BooleanArray: true if target_pk IS NULL
                let is_null_any: ArrayRef =
                    compute::is_null(pk_array).map_err(internal_err!("..."))?;
                let is_null: &BooleanArray = is_null_any
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("is_null must produce BooleanArray");

                // build two Int32Array masks: one for “1” (insert), one for “0” (update)
                let num_rows = batch.num_rows();
                let ones = Int32Array::from(vec![1; num_rows]);
                let zeros = Int32Array::from(vec![0; num_rows]);

                let op_any: ArrayRef = compute::if_then_else(is_null, &ones, &zeros)
                    .map_err(DataFusionError::ArrowError)?;

                let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
                new_columns.extend_from_slice(batch.columns());
                new_columns.push(op_any);

                // build a new RecordBatch with the final scheam
                let out_batch = RecordBatch::try_new(this.output_schema.clone(), new_columns)
                    .map_err(internal_err!("faile batch schema"))?;

                Poll::Ready(Some(Ok(out_batch)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}
