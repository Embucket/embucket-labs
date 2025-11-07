use super::catalog::SQLITE_CATALOG;
use crate::catalogs::sqlite::databases::DatabasesView;
use crate::catalogs::sqlite::metastore_config::MetastoreViewConfig;
use crate::catalogs::sqlite::schemas::SchemasView;
use crate::catalogs::sqlite::tables::TablesView;
use crate::catalogs::sqlite::volumes::VolumesView;
use async_trait::async_trait;
use core_metastore::Metastore;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::streaming::PartitionStream;
use std::any::Any;
use std::sync::Arc;

pub const DATABASES: &str = "databases";
pub const VOLUMES: &str = "volumes";
pub const SCHEMAS: &str = "schemas";
pub const TABLES: &str = "tables";

pub const METASTORE_VIEW_TABLES: &[&str] = &[TABLES, SCHEMAS, DATABASES, VOLUMES];

pub struct MetastoreViewSchemaProvider {
    config: MetastoreViewConfig,
}

impl MetastoreViewSchemaProvider {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self {
            config: MetastoreViewConfig {
                database: SQLITE_CATALOG.to_string(),
                metastore,
            },
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for MetastoreViewSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreSchema").finish()
    }
}

#[async_trait]
impl SchemaProvider for MetastoreViewSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        METASTORE_VIEW_TABLES
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let config = self.config.clone();
        let table: Arc<dyn PartitionStream> = match name.to_ascii_lowercase().as_str() {
            DATABASES => Arc::new(DatabasesView::new(config)),
            VOLUMES => Arc::new(VolumesView::new(config)),
            SCHEMAS => Arc::new(SchemasView::new(config)),
            TABLES => Arc::new(TablesView::new(config)),
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(StreamingTable::try_new(
            Arc::clone(table.schema()),
            vec![table],
        )?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        METASTORE_VIEW_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}
