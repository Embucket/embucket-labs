use crate::catalogs::slatedb::history_store_schema::HistoryStoreViewSchemaProvider;
use crate::catalogs::slatedb::metastore_schema::MetastoreViewSchemaProvider;
use core_history::HistoryStore;
use core_metastore::Metastore;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{any::Any, sync::Arc};

pub const SLATEDB_CATALOG: &str = "sqlite";
pub const METASTORE_SCHEMA: &str = "meta";
pub const HISTORY_STORE_SCHEMA: &str = "history";
pub const SLATEDB_SCHEMAS: &[&str] = &[METASTORE_SCHEMA, HISTORY_STORE_SCHEMA];

#[derive(Clone, Debug)]
pub struct SlateDBCatalog {
    pub metastore: Arc<dyn Metastore>,
    pub history_store: Arc<dyn HistoryStore>,
}

impl SlateDBCatalog {
    pub fn new(metastore: Arc<dyn Metastore>, history_store: Arc<dyn HistoryStore>) -> Self {
        Self {
            metastore,
            history_store,
        }
    }
}

impl CatalogProvider for SlateDBCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        SLATEDB_SCHEMAS.iter().map(ToString::to_string).collect()
    }

    #[allow(clippy::as_conversions)]
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            METASTORE_SCHEMA => Some(Arc::new(MetastoreViewSchemaProvider::new(Arc::clone(
                &self.metastore,
            )))),
            HISTORY_STORE_SCHEMA => Some(Arc::new(HistoryStoreViewSchemaProvider::new(
                Arc::clone(&self.history_store),
            ))),
            _ => None,
        }
    }
}
