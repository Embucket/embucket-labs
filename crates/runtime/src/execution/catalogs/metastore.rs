// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::execution::catalogs::catalog::DFCatalog;
use crate::execution::catalogs::iceberg_catalog::IcebergBridge;
use crate::execution::error::{self as ex_error, ExecutionResult};
use dashmap::DashMap;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, TableProvider},
    datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl},
    execution::{object_store::ObjectStoreRegistry, options::ReadOptions},
    prelude::{ParquetReadOptions, SessionContext},
};
use datafusion_common::DataFusionError;
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use embucket_metastore::{error::MetastoreError, Metastore};
use embucket_utils::list_config::ListConfig;
use iceberg_rust::{
    catalog::Catalog as IcebergCatalog, spec::identifier::Identifier as IcebergIdentifier,
};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use snafu::ResultExt;
use url::Url;

pub const DEFAULT_CATALOG: &str = "embucket";

pub type TableProviderCache = DashMap<String, Arc<dyn TableProvider>>;
pub type SchemaProviderCache = DashMap<String, TableProviderCache>;
pub type CatalogProviderCache = DashMap<String, SchemaProviderCache>;

#[derive(Clone)]
pub struct DFMetastore {
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<CatalogProviderCache>,
    pub table_object_store: Arc<DashMap<String, Arc<dyn ObjectStore>>>,
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl DFMetastore {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        let table_object_store: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        table_object_store.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        Self {
            metastore,
            mirror: Arc::new(DashMap::new()),
            table_object_store: Arc::new(table_object_store),
            catalogs: DashMap::default(),
        }
    }

    #[allow(clippy::as_conversions, clippy::too_many_lines)]
    pub async fn refresh(&self, ctx: &SessionContext) -> ExecutionResult<()> {
        let mut seen: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::default();

        let databases = self
            .metastore
            .list_databases(ListConfig::default())
            .await
            .context(ex_error::MetastoreSnafu)?;
        for database in databases {
            let db_entry = self
                .mirror
                .entry(database.ident.clone())
                .or_insert(SchemaProviderCache::default());
            let db_seen_entry = seen.entry(database.ident.clone()).or_default();
            let schemas = self
                .metastore
                .list_schemas(&database.ident, ListConfig::default())
                .await
                .context(ex_error::MetastoreSnafu)?;
            for schema in schemas {
                let schema_entry = db_entry
                    .entry(schema.ident.schema.clone())
                    .insert(TableProviderCache::default());
                let schema_seen_entry = db_seen_entry
                    .entry(schema.ident.schema.clone())
                    .or_default();
                let tables = self
                    .metastore
                    .list_tables(&schema.ident, ListConfig::default())
                    .await
                    .context(ex_error::MetastoreSnafu)?;
                for table in tables {
                    let table_url = self
                        .metastore
                        .url_for_table(&table.ident)
                        .await
                        .context(ex_error::MetastoreSnafu)?;
                    let table_object_store = self
                        .metastore
                        .table_object_store(&table.ident)
                        .await
                        .context(ex_error::MetastoreSnafu)?
                        .ok_or(MetastoreError::TableObjectStoreNotFound {
                            table: table.ident.table.clone(),
                            schema: table.ident.schema.clone(),
                            db: table.ident.database.clone(),
                        })
                        .context(ex_error::MetastoreSnafu)?;
                    let url = Url::parse(&table_url).context(ex_error::UrlParseSnafu)?;
                    self.table_object_store
                        .insert(get_url_key(&url), table_object_store.clone());

                    let table_provider = match table.format {
                        embucket_metastore::TableFormat::Parquet => {
                            let parq_read_options = ParquetReadOptions::default();
                            let listing_options = parq_read_options.to_listing_options(
                                ctx.state().config(),
                                ctx.state().default_table_options(),
                            );

                            let table_path = ListingTableUrl::parse(&table_url)
                                .context(ex_error::DataFusionSnafu)?;

                            // TODO: Switch Metastore to use arrow schema instead and just use that instead of scanning
                            let schema = listing_options
                                .infer_schema(&ctx.state(), &table_path)
                                .await
                                .context(ex_error::DataFusionSnafu)?;
                            let config = ListingTableConfig::new(table_path)
                                .with_listing_options(listing_options)
                                .with_schema(schema);
                            Arc::new(
                                ListingTable::try_new(config).context(ex_error::DataFusionSnafu)?,
                            ) as Arc<dyn TableProvider>
                        }
                        embucket_metastore::TableFormat::Iceberg => {
                            let bridge = Arc::new(IcebergBridge {
                                metastore: self.metastore.clone(),
                                database: table.ident.clone().database,
                                object_store: table_object_store.clone(),
                            });

                            let ib_identifier = IcebergIdentifier::new(
                                &[table.ident.schema.clone()],
                                &table.ident.table,
                            );
                            let tabular = bridge
                                .load_tabular(&ib_identifier)
                                .await
                                .map_err(|e| DataFusionError::External(Box::new(e)))
                                .context(ex_error::DataFusionSnafu)?;
                            Arc::new(IcebergDataFusionTable::new(tabular, None, None, None))
                                as Arc<dyn TableProvider>
                        }
                    };
                    //mirror_entry.insert(table.ident.table.clone());
                    schema_entry.insert(table.ident.table.clone(), table_provider.clone());
                    schema_seen_entry.insert(table.ident.table.clone());
                }
            }
        }

        for db in self.mirror.iter_mut() {
            if seen.contains_key(db.key()) {
                for schema in db.iter_mut() {
                    if seen[db.key()].contains_key(schema.key()) {
                        let mut tables_to_remove = vec![];
                        for table in schema.iter() {
                            if !seen[db.key()][schema.key()].contains(table.key()) {
                                tables_to_remove.push(table.key().clone());
                            }
                        }

                        for table in tables_to_remove {
                            schema.remove(&table);
                        }
                    } else {
                        db.remove(schema.key());
                    }
                }
            } else {
                self.mirror.remove(db.key());
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for DFMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFMetastore").finish()
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
#[must_use]
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}

impl ObjectStoreRegistry for DFMetastore {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let url = get_url_key(url);
        self.table_object_store.insert(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion_common::Result<Arc<dyn ObjectStore>> {
        let url = get_url_key(url);
        if let Some(object_store) = self.table_object_store.get(&url) {
            Ok(object_store.clone())
        } else {
            Err(DataFusionError::Execution(format!(
                "Object store not found for url {url}"
            )))
        }
    }
}

// Explore using AsyncCatalogProviderList alongside CatalogProviderList
impl CatalogProviderList for DFMetastore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut catalog_names = self
            .catalogs
            .iter()
            .map(|c| c.key().clone())
            .collect::<Vec<String>>();
        catalog_names.extend(self.mirror.iter().map(|e| e.key().clone()));
        catalog_names
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if let Some(catalog) = self.catalogs.get(name) {
            return Some(catalog.value().clone());
        }
        if !self.mirror.contains_key(name) {
            return None;
        }
        let iceberg_catalog = IcebergBridge::new(self.metastore.clone(), name.to_string())
            .ok()
            .map(Arc::new)?;
        let catalog: Arc<dyn CatalogProvider> = Arc::new(DFCatalog {
            ident: name.to_string(),
            metastore: self.metastore.clone(),
            mirror: self.mirror.clone(),
            iceberg_catalog,
        });
        Some(catalog)
    }
}
