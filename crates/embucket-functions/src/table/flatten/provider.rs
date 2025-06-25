use crate::json::{get_json_value, PathToken};
use crate::table::flatten::func::FlattenTableFunc;
use arrow_schema::{DataType, Field, Fields, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder, UInt64Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{
    exec_err, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, TableReference,
};
use datafusion_expr::{EmptyRelation, Projection, TableType};
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use serde_json::Value;
use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum FlattenMode {
    Both,
    Array,
    Object,
}

impl FlattenMode {
    pub const fn is_object(self) -> bool {
        matches!(self, Self::Object | Self::Both)
    }

    pub const fn is_array(self) -> bool {
        matches!(self, Self::Array | Self::Both)
    }
}

#[derive(Debug, Clone)]
pub struct FlattenArgs {
    pub input_expr: Expr,
    pub path: Vec<PathToken>,
    pub is_outer: bool,
    pub is_recursive: bool,
    pub mode: FlattenMode,
}

pub struct Out {
    pub seq: UInt64Builder,
    pub key: StringBuilder,
    pub path: StringBuilder,
    pub index: UInt64Builder,
    pub value: StringBuilder,
    pub this: StringBuilder,
    pub last_outer: Option<Value>,
}

#[derive(Debug)]
pub struct FlattenTableProvider {
    pub args: FlattenArgs,
    pub schema: Arc<DFSchema>,
}

#[async_trait]
impl TableProvider for FlattenTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.inner().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected SessionState in flatten".to_string())
            })?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(self.schema.inner().clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Arc::new(FlattenExec {
            args: self.args.clone(),
            schema: self.schema.clone(),
            session_state: Arc::new(session_state.clone()),
            properties,
        }))
    }
}

pub struct FlattenExec {
    args: FlattenArgs,
    schema: Arc<DFSchema>,
    session_state: Arc<SessionState>,
    properties: PlanProperties,
}

impl Debug for FlattenExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FlattenExec")
    }
}

impl DisplayAs for FlattenExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FlattenExec")
    }
}

impl ExecutionPlan for FlattenExec {
    fn name(&self) -> &'static str {
        "FlattenExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.inner().clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            session_state: self.session_state.clone(),
            args: self.args.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batches = self.get_input_batches(partition, context)?;
        let flatten_func = FlattenTableFunc::new();

        let mut all_batches = vec![];
        let mut last_outer: Option<Value> = None;
        for batch in batches {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected input column to be Utf8".to_string())
                })?;

            flatten_func.row_id.fetch_add(1, Ordering::Acquire);

            let out = Rc::new(RefCell::new(Out {
                seq: UInt64Builder::new(),
                key: StringBuilder::new(),
                path: StringBuilder::new(),
                index: UInt64Builder::new(),
                value: StringBuilder::new(),
                this: StringBuilder::new(),
                last_outer: None,
            }));

            for i in 0..array.len() {
                let json_str = array.value(i);
                let json_val: Value = serde_json::from_str(json_str)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid JSON input: {e}")))?;

                let Some(input) = get_json_value(&json_val, &self.args.path) else {
                    continue;
                };

                flatten_func.flatten(
                    input,
                    &self.args.path,
                    self.args.is_outer,
                    self.args.is_recursive,
                    &self.args.mode,
                    &out,
                )?;
            }

            let mut out = out.borrow_mut();
            let cols: Vec<ArrayRef> = vec![
                Arc::new(out.seq.finish()),
                Arc::new(out.key.finish()),
                Arc::new(out.path.finish()),
                Arc::new(out.index.finish()),
                Arc::new(out.value.finish()),
                Arc::new(out.this.finish()),
            ];

            last_outer.clone_from(&out.last_outer);
            let batch = RecordBatch::try_new(self.schema.inner().clone(), cols)?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(
                vec![flatten_func.empty_record_batch(
                    self.schema.inner().clone(),
                    &self.args.path,
                    last_outer,
                    self.args.is_outer,
                )],
                self.schema.inner().clone(),
                None,
            )?));
        }

        Ok(Box::pin(MemoryStream::try_new(
            all_batches,
            self.schema.inner().clone(),
            None,
        )?))
    }
}

impl FlattenExec {
    fn get_input_batches(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        if let Expr::Literal(ScalarValue::Utf8(Some(s))) = self.args.input_expr.clone() {
            let array: ArrayRef = Arc::new(StringArray::from(vec![s]));
            let batch = RecordBatch::try_from_iter(vec![("input", array)])?;
            Ok(vec![batch])
        } else {
            let expr = self.args.input_expr.clone();
            let session_state = self.session_state.clone();
            futures::executor::block_on(async move {
                let schema = build_schema_from_expr(&expr)?;
                let empty_plan = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: true,
                    schema: schema.clone(),
                });
                let projection_plan = LogicalPlan::Projection(Projection::try_new_with_schema(
                    vec![expr.alias("input")],
                    Arc::new(empty_plan),
                    schema,
                )?);
                let plan = session_state.create_physical_plan(&projection_plan).await?;
                let input_stream = plan.execute(partition, context)?;
                collect(input_stream).await
            })
        }
    }
}

fn build_schema_from_expr(expr: &Expr) -> Result<DFSchemaRef> {
    let mut columns = HashSet::new();
    expr.apply(&mut |e: &Expr| {
        if let Expr::Column(col) = e {
            columns.insert(col.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    if columns.is_empty() {
        let field = Field::new("input", DataType::Utf8, true);
        return Ok(Arc::new(DFSchema::from_unqualified_fields(
            Fields::from(vec![field]),
            HashMap::default(),
        )?));
    }

    let qualified_fields = columns
        .into_iter()
        .map(|col| {
            (
                col.relation,
                Arc::new(Field::new(col.name, DataType::Utf8, true)),
            )
        })
        .collect::<Vec<(Option<TableReference>, Arc<Field>)>>();
    Ok(Arc::new(DFSchema::new_with_metadata(
        qualified_fields,
        HashMap::default(),
    )?))
}
