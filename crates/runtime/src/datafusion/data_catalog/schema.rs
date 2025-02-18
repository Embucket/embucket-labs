use crate::datafusion::data_catalog::extended_mirror::ExtendedMirror;
use datafusion_catalog::memory::MemorySchemaProvider;
use datafusion::datasource::MemTable;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use datafusion_iceberg::catalog::schema::IcebergSchema;
use iceberg_rust::catalog::namespace::Namespace;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// SchemaProvider supporting dynamic type-based table categorization
#[derive(Debug)]
pub struct MultiSchemaProvider {
    memory_schema: Arc<MemorySchemaProvider>,
    iceberg_schema: Arc<IcebergSchema>,
}

impl MultiSchemaProvider {
    pub fn new(schema: Namespace, catalog: Arc<ExtendedMirror>) -> Self {
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
            .chain(memory_tables.into_iter())
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
        match table.as_any().is::<MemTable>() {
            true => self.memory_schema.register_table(name, table),
            false => self.iceberg_schema.register_table(name, table),
        }
        /*let mut full_name = self.schema.to_vec();
        full_name.push(name.to_owned());
        let identifier = Identifier::try_new(&full_name, None)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        self.catalog.register_table(identifier, table)*/
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
