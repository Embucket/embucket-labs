use crate::execution::catalog::schema::CachingSchema;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{any::Any, sync::Arc};

#[derive(Clone)]
pub struct CachingCatalog {
    pub catalog: Arc<dyn CatalogProvider>,
    pub schemas_cache: DashMap<String, Arc<CachingSchema>>,
    pub should_refresh: bool,
    pub name: String,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CachingCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("name", &self.name)
            .field("should_refresh", &self.should_refresh)
            .field("schemas_cache", &self.schemas_cache)
            .field("catalog", &"")
            .finish()
    }
}

impl CatalogProvider for CachingCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        if self.schemas_cache.is_empty() {
            // Fallback to the original catalog schema names if the cache is empty
            self.catalog.schema_names()
        } else {
            self.schemas_cache
                .iter()
                .map(|entry| entry.key().clone())
                .collect()
        }
    }

    #[allow(clippy::as_conversions)]
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas_cache
            .get(name)
            .map(|schema| Arc::clone(schema.value()) as Arc<dyn SchemaProvider>)
            // Fallback to the original catalog schema if the cache is empty
            .or_else(|| self.catalog.schema(name))
    }
}
