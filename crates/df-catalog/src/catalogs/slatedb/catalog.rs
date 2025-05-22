use crate::catalogs::slatedb::schema::SlateDBViewSchemaProvider;
use core_metastore::Metastore;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{any::Any, sync::Arc};

pub const SLATEDB_CATALOG: &str = "slatedb";
pub const SLATEDB_SCHEMA: &str = "public";

#[derive(Clone, Debug)]
pub struct SlateDBCatalog {
    pub metastore: Arc<dyn Metastore>,
}

impl SlateDBCatalog {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self { metastore }
    }
}

impl CatalogProvider for SlateDBCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![SLATEDB_SCHEMA.to_string()]
    }

    #[allow(clippy::as_conversions)]
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name != SLATEDB_SCHEMA {
            return None;
        }
        Some(Arc::new(SlateDBViewSchemaProvider::new(Arc::clone(
            &self.metastore,
        ))))
    }
}
