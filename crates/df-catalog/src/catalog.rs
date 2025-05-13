use crate::schema::CachingSchema;
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
        let schema_names = self.catalog.schema_names();

        // Remove outdated records
        let schema_names_set: std::collections::HashSet<_> = schema_names.iter().cloned().collect();
        self.schemas_cache
            .retain(|name, _| schema_names_set.contains(name));

        for name in &schema_names {
            if self.schemas_cache.contains_key(name) {
                continue;
            }

            if let Some(schema) = self.catalog.schema(name) {
                self.schemas_cache.insert(
                    name.clone(),
                    Arc::new(CachingSchema {
                        name: name.clone(),
                        schema,
                        tables_cache: DashMap::new(),
                    }),
                );
            }
        }
        schema_names
    }

    #[allow(clippy::as_conversions)]
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(schema) = self.schemas_cache.get(name) {
            Some(Arc::clone(schema.value()) as Arc<dyn SchemaProvider>)
        } else if let Some(schema) = self.catalog.schema(name) {
            let caching_schema = Arc::new(CachingSchema {
                name: name.to_string(),
                schema: Arc::clone(&schema),
                tables_cache: DashMap::new(),
            });

            self.schemas_cache
                .insert(name.to_string(), Arc::clone(&caching_schema));
            Some(caching_schema as Arc<dyn SchemaProvider>)
        } else {
            None
        }
    }
}
