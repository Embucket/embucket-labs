use crate::execution::catalog::table::CachingTable;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use std::any::Any;
use std::sync::Arc;

pub struct CachingSchema {
    pub schema: Arc<dyn SchemaProvider>,
    pub name: String,
    pub tables_cache: DashMap<String, Arc<CachingTable>>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CachingSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema")
            .field("schema", &"")
            .field("name", &self.name)
            .field("tables_cache", &self.tables_cache)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for CachingSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        if self.tables_cache.is_empty() {
            self.schema.table_names()
        } else {
            // Don't fill the cache since should call async table() to fill it
            self.tables_cache
                .iter()
                .map(|entry| entry.key().clone())
                .collect()
        }
    }

    #[allow(clippy::as_conversions)]
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables_cache.get(name) {
            Ok(Some(Arc::clone(table.value()) as Arc<dyn TableProvider>))
        } else {
            // Fallback to the original schema table if the cache is empty
            if let Some(table) = self.schema.table(name).await? {
                let caching_table =
                    Arc::new(CachingTable::new(name.to_string(), Arc::clone(&table)));

                // Insert into cache
                self.tables_cache
                    .insert(name.to_string(), Arc::clone(&caching_table));

                Ok(Some(caching_table as Arc<dyn TableProvider>))
            } else {
                Ok(None)
            }
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        let caching_table = Arc::new(CachingTable::new(name.clone(), Arc::clone(&table)));
        self.tables_cache.insert(name.clone(), caching_table);
        self.schema.register_table(name, table)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.tables_cache.remove(name);
        self.schema.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables_cache.contains_key(name)
    }
}
