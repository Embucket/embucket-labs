use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Constraints;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::Expr;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct TemporaryMemTable {
    inner: MemTable,
}

impl TemporaryMemTable {
    pub fn try_new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        MemTable::try_new(schema, partitions).map(|inner| Self { inner })
    }
}

#[async_trait]
impl TableProvider for TemporaryMemTable {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }
}
