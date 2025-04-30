use super::TableProviderCache;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use embucket_metastore::Metastore;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust_spec::identifier::Identifier;
use std::any::Any;
use std::sync::Arc;

pub struct EmbucketSchema {
    pub database: String,
    pub schema: String,
    pub metastore: Arc<dyn Metastore>,
    pub tables_cache: Arc<TableProviderCache>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for EmbucketSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFSchema")
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("metastore", &"")
            .field("iceberg_catalog", &"")
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for EmbucketSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables_cache
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables_cache.get(name) {
            return Ok(Some(table.clone()));
        }

        let Ok(tabular) = self
            .iceberg_catalog
            .clone()
            .load_tabular(&Identifier::new(&[self.schema.clone()], name))
            .await
        else {
            return Ok(None);
        };

        let table_provider: Arc<dyn TableProvider> =
            Arc::new(IcebergDataFusionTable::new(tabular, None, None, None));
        self.tables_cache
            .insert(name.to_string(), table_provider.clone());
        Ok(Option::from(table_provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables_cache.contains_key(name)
    }
}
