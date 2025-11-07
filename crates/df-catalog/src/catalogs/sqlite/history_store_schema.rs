use super::catalog::SQLITE_CATALOG;
use crate::catalogs::sqlite::history_store_config::HistoryStoreViewConfig;
use crate::catalogs::sqlite::queries::QueriesView;
use crate::catalogs::sqlite::worksheets::WorksheetsView;
use async_trait::async_trait;
use core_history::HistoryStore;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::streaming::PartitionStream;
use std::any::Any;
use std::sync::Arc;

pub const WORKSHEETS: &str = "worksheets";
pub const QUERIES: &str = "queries";
pub const HISTORY_STORE_VIEW_TABLES: &[&str] = &[WORKSHEETS, QUERIES];

pub struct HistoryStoreViewSchemaProvider {
    config: HistoryStoreViewConfig,
}

impl HistoryStoreViewSchemaProvider {
    pub fn new(history_store: Arc<dyn HistoryStore>) -> Self {
        Self {
            config: HistoryStoreViewConfig {
                database: SQLITE_CATALOG.to_string(),
                history_store,
            },
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for HistoryStoreViewSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryStoreSchema").finish()
    }
}

#[async_trait]
impl SchemaProvider for HistoryStoreViewSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        HISTORY_STORE_VIEW_TABLES
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[tracing::instrument(
        name = "HistoryStoreViewSchemaProvider::table",
        level = "debug",
        skip(self),
        err
    )]
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let config = self.config.clone();
        let table: Arc<dyn PartitionStream> = match name.to_ascii_lowercase().as_str() {
            WORKSHEETS => Arc::new(WorksheetsView::new(config)),
            QUERIES => Arc::new(QueriesView::new(config)),
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(StreamingTable::try_new(
            Arc::clone(table.schema()),
            vec![table],
        )?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        HISTORY_STORE_VIEW_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}
