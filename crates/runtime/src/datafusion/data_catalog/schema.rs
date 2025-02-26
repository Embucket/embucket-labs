use crate::datafusion::data_catalog::extended_catalog::ExtendedIcebergCatalog;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use datafusion_expr::TableType;
use datafusion_iceberg::catalog::schema::IcebergSchema;
use iceberg_rust::catalog::namespace::Namespace;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiSchemaProvider {
    memory_schema: Arc<MemorySchemaProvider>,
    iceberg_schema: Arc<IcebergSchema>,
}

impl MultiSchemaProvider {
    #[must_use]
    pub fn new(schema: Namespace, catalog: Arc<ExtendedIcebergCatalog>) -> Self {
        Self {
            memory_schema: Arc::new(MemorySchemaProvider::new()),
            iceberg_schema: Arc::new(IcebergSchema::new(schema, catalog.base.clone())),
        }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for MultiSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        let iceberg_tables = self.iceberg_schema.table_names();
        let memory_tables = self.memory_schema.table_names();
        iceberg_tables
            .into_iter()
            .chain(memory_tables)
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let iceberg_table = self.iceberg_schema.table(name).await;
        if iceberg_table.is_ok() {
            iceberg_table
        } else {
            self.memory_schema.table(name).await
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        match table.table_type() {
            TableType::Temporary => self.memory_schema.register_table(name, table),
            TableType::Base => self.iceberg_schema.register_table(name, table),
            _ => Err(DataFusionError::Execution(
                "Unsupported table type for registration".to_string(),
            )),
        }
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match self.iceberg_schema.deregister_table(name) {
            Ok(table) => Ok(table),
            Err(_) => self.memory_schema.deregister_table(name),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.iceberg_schema.table_exist(name) || self.memory_schema.table_exist(name)
    }
}
