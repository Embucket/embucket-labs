use super::schema::EmbucketSchema;
use crate::catalogs::embucket::block_in_new_runtime;
use core_metastore::{Metastore, SchemaIdent};
use core_utils::scan_iterator::ScanIterator;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use std::{any::Any, sync::Arc};

pub struct EmbucketCatalog {
    pub database: String,
    pub metastore: Arc<dyn Metastore>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
}

impl EmbucketCatalog {
    pub fn new(
        database: String,
        metastore: Arc<dyn Metastore>,
        iceberg_catalog: Arc<dyn IcebergCatalog>,
    ) -> Self {
        Self {
            database,
            metastore,
            iceberg_catalog,
        }
    }

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
        let metastore = self.metastore.clone();
        let database = self.database.clone();

        block_in_new_runtime(async move {
            match metastore.iter_schemas(&database).collect().await {
                Ok(schemas) => schemas
                    .into_iter()
                    .map(|s| s.ident.schema.clone())
                    .collect(),
                Err(_) => vec![],
            }
        })
        .unwrap_or_else(|_| vec![])
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let metastore = self.metastore.clone();
        let iceberg_catalog = self.iceberg_catalog.clone();
        let database = self.database.clone();
        let schema_name = name.to_string();

        block_in_new_runtime(async move {
            match metastore
                .get_schema(&SchemaIdent::new(database.clone(), schema_name.clone()))
                .await
            {
                Ok(_) => {
                    let schema = EmbucketSchema {
                        database,
                        schema: schema_name,
                        metastore,
                        iceberg_catalog,
                    };
                    let arc: Arc<dyn SchemaProvider> = Arc::new(schema);
                    Some(arc)
                }
                Err(_) => None,
            }
        })
        .unwrap_or(None)
    }
}
