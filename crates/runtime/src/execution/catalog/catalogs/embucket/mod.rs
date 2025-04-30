use dashmap::DashMap;
use datafusion::catalog::TableProvider;
use std::sync::Arc;

pub mod catalog;
pub mod iceberg_catalog;
pub mod schema;

pub type TableProviderCache = DashMap<String, Arc<dyn TableProvider>>;
pub type SchemaProviderCache = DashMap<String, Arc<TableProviderCache>>;
pub type CatalogProviderCache = DashMap<String, Arc<SchemaProviderCache>>;
