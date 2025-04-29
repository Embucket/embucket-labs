use std::{any::Any, sync::Arc};

use super::schema::EmbucketSchema;
use super::SchemaProviderCache;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use embucket_metastore::Metastore;
use iceberg_rust::catalog::Catalog as IcebergCatalog;

pub struct EmbucketCatalog {
    pub database: String,
    pub metastore: Arc<dyn Metastore>,
    pub schemas_cache: Arc<SchemaProviderCache>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
}

impl EmbucketCatalog {
    #[must_use]
    pub fn catalog(&self) -> Arc<dyn IcebergCatalog> {
        self.iceberg_catalog.clone()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for EmbucketCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFCatalog")
            .field("database", &self.database)
            .field("iceberg_catalog", &"")
            .finish()
    }
}

impl CatalogProvider for EmbucketCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas_cache
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas_cache.get(name).map(|tables_cache| {
            let tables_cache = Arc::clone(tables_cache.value());
            let provider: Arc<dyn SchemaProvider> = Arc::new(EmbucketSchema {
                database: self.database.clone(),
                schema: name.to_string(),
                metastore: self.metastore.clone(),
                tables_cache,
                iceberg_catalog: self.iceberg_catalog.clone(),
            });
            provider
        })
    }
}
