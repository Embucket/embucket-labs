use super::schema::EmbucketSchema;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use embucket_metastore::{Metastore, SchemaIdent};
use embucket_utils::scan_iterator::ScanIterator;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use std::{any::Any, sync::Arc};

pub struct EmbucketCatalog {
    pub database: String,
    pub metastore: Arc<dyn Metastore>,
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
        let metastore = self.metastore.clone();
        let database = self.database.clone();

        std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Runtime::new() else {
                return vec![];
            };
            rt.block_on(async {
                match metastore.iter_schemas(&database).collect().await {
                    Ok(schemas) => schemas
                        .into_iter()
                        .map(|s| s.ident.schema.clone())
                        .collect(),
                    Err(_) => vec![],
                }
            })
        })
        .join()
        .unwrap_or_else(|_| vec![])
    }

    #[allow(clippy::as_conversions)]
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let metastore = self.metastore.clone();
        let iceberg_catalog = self.iceberg_catalog.clone();
        let database = self.database.clone();
        let schema_name = name.to_string();

        std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Runtime::new() else {
                return None;
            };
            rt.block_on(async {
                match metastore
                    .get_schema(&SchemaIdent::new(database.clone(), schema_name.clone()))
                    .await
                {
                    Ok(_) => Some(Arc::new(EmbucketSchema {
                        database,
                        schema: schema_name,
                        metastore,
                        iceberg_catalog,
                    }) as Arc<dyn SchemaProvider>),
                    Err(_) => None,
                }
            })
        })
        .join()
        .unwrap_or(None)
    }
}
