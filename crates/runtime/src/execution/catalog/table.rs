use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::ExecutionPlan;
use once_cell::sync::OnceCell;
use std::any::Any;
use std::sync::Arc;

pub struct CachingTable {
    pub schema: OnceCell<SchemaRef>,
    pub name: String,
    pub table: Arc<dyn TableProvider>,
}

impl CachingTable {
    pub fn new(name: String, table: Arc<dyn TableProvider>) -> Self {
        Self {
            schema: OnceCell::new(),
            name,
            table,
        }
    }
    pub fn new_with_schema(name: String, schema: SchemaRef, table: Arc<dyn TableProvider>) -> Self {
        Self {
            schema: OnceCell::from(schema),
            name,
            table,
        }
    }
}

impl std::fmt::Debug for CachingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("schema", &"")
            .field("name", &self.name)
            .field("table", &"")
            .finish()
    }
}

#[async_trait]
impl TableProvider for CachingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.get_or_init(|| self.table.schema()).clone()
    }

    fn table_type(&self) -> TableType {
        self.table.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.table.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.table.insert_into(state, input, insert_op).await
    }
}
