use crate::catalogs::slatedb::config::SlateDBViewConfig;
use crate::catalogs::slatedb::databases::DatabasesView;
use crate::catalogs::slatedb::schemas::SchemasView;
use crate::catalogs::slatedb::volumes::VolumesView;
use async_trait::async_trait;
use core_metastore::Metastore;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::streaming::PartitionStream;
use std::any::Any;
use std::sync::Arc;

pub const SLATEDB_SCHEMA: &str = "public";
pub const SLATEDB_CATALOG: &str = "slatedb";
pub const DATABASES: &str = "databases";
pub const VOLUMES: &str = "volumes";
pub const SCHEMAS: &str = "schemas";

pub const VIEW_TABLES: &[&str] = &[SCHEMAS, DATABASES, VOLUMES];

pub struct SlateDBViewSchemaProvider {
    config: SlateDBViewConfig,
}

impl SlateDBViewSchemaProvider {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self {
            config: SlateDBViewConfig {
                database: SLATEDB_CATALOG.to_string(),
                metastore,
            },
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SlateDBViewSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBSchema").finish()
    }
}

#[async_trait]
impl SchemaProvider for SlateDBViewSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        VIEW_TABLES.iter().map(ToString::to_string).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let config = self.config.clone();
        let table: Arc<dyn PartitionStream> = match name.to_ascii_lowercase().as_str() {
            DATABASES => Arc::new(DatabasesView::new(config)),
            VOLUMES => Arc::new(VolumesView::new(config)),
            SCHEMAS => Arc::new(SchemasView::new(config)),
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(StreamingTable::try_new(
            Arc::clone(table.schema()),
            vec![table],
        )?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        VIEW_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}
