use crate::schema::CachingSchema;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{any::Any, sync::Arc};

#[derive(Clone)]
pub struct CachingCatalog {
    pub catalog: Arc<dyn CatalogProvider>,
    pub catalog_type: CatalogType,
    pub schemas_cache: DashMap<String, Arc<CachingSchema>>,
    pub should_refresh: bool,
    pub name: String,
    pub enable_information_schema: bool,
}

#[derive(Clone, Debug)]
pub enum CatalogType {
    Internal,
    Memory,
    S3tables,
}

impl CachingCatalog {
    pub fn new(catalog: Arc<dyn CatalogProvider>, name: String) -> Self {
        Self {
            catalog,
            schemas_cache: DashMap::new(),
            should_refresh: false,
            enable_information_schema: true,
            name,
            catalog_type: CatalogType::Internal,
        }
    }
    #[must_use]
    pub const fn with_refresh(mut self, refresh: bool) -> Self {
        self.should_refresh = refresh;
        self
    }
    #[must_use]
    pub const fn with_information_schema(mut self, enable_information_schema: bool) -> Self {
        self.enable_information_schema = enable_information_schema;
        self
    }

    #[must_use]
    pub const fn with_catalog_type(mut self, catalog_type: CatalogType) -> Self {
        self.catalog_type = catalog_type;
        self
    }
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

    #[tracing::instrument(
        name = "CachingCatalog::schema_names",
        level = "debug",
        skip(self),
        fields(schemas_names_count, catalog_name=format!("{:?}", self.name)),
    )]
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
        // Record the result as part of the current span.
        tracing::Span::current().record("schemas_names_count", schema_names.len());

        schema_names
    }

    #[tracing::instrument(name = "CachingCatalog::schema", level = "debug", skip(self))]
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

    #[tracing::instrument(
        name = "CachingCatalog::register_schema",
        level = "debug",
        skip(self),
        fields(schemas_names_count, catalog_name=format!("{:?}", self.name)),
    )]
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        let caching_schema = Arc::new(CachingSchema {
            name: name.to_string(),
            schema: Arc::clone(&schema),
            tables_cache: DashMap::new(),
        });
        self.schemas_cache
            .insert(name.to_string(), Arc::clone(&caching_schema));
        self.catalog.register_schema(name, schema)
    }

    #[tracing::instrument(
        name = "CachingCatalog::deregister_schema",
        level = "debug",
        skip(self),
        fields(schemas_names_count, catalog_name=format!("{:?}", self.name)),
    )]
    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        self.schemas_cache.remove(name);
        self.catalog.deregister_schema(name, cascade)
    }
}
