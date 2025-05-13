use super::catalogs::embucket::catalog::EmbucketCatalog;
use super::catalogs::embucket::iceberg_catalog::EmbucketIcebergCatalog;
use crate::catalog::CachingCatalog;
use crate::error::{DataFusionSnafu, Error, MetastoreSnafu, Result, S3TablesSnafu};
use crate::schema::CachingSchema;
use crate::table::CachingTable;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use core_metastore::{AwsCredentials, Metastore, VolumeType as MetastoreVolumeType};
use core_utils::scan_iterator::ScanIterator;
use dashmap::DashMap;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList},
    execution::object_store::ObjectStoreRegistry,
};
use datafusion_common::DataFusionError;
use datafusion_iceberg::catalog::catalog::IcebergCatalog as DataFusionIcebergCatalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_s3tables_catalog::S3TablesCatalog;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;
use url::Url;

pub const DEFAULT_CATALOG: &str = "embucket";

pub struct EmbucketCatalogList {
    pub metastore: Arc<dyn Metastore>,
    pub table_object_store: Arc<DashMap<String, Arc<dyn ObjectStore>>>,
    pub catalogs: DashMap<String, Arc<CachingCatalog>>,
}

impl EmbucketCatalogList {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        let table_object_store: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        table_object_store.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        Self {
            metastore,
            table_object_store: Arc::new(table_object_store),
            catalogs: DashMap::default(),
        }
    }

    /// Discovers and registers all available catalogs into the catalog registry.
    ///
    /// This method retrieves internal catalogs from the metastore as well as external
    /// catalogs configured by the metastore volumes. Both sets of catalogs
    /// are collected and inserted into the local `catalogs` registry, making them
    /// available for use by the query engine.
    ///
    /// Internal catalogs are typically backed by a database and support Iceberg tables.
    /// S3 tables catalogs are derived from metastore volume definitions of type `S3Tables`,
    /// and provide access to tables stored in S3-compatible object storage.
    /// # Errors
    ///
    /// This method can return errors related to:
    /// - Metastore access (e.g., during database or volume listing)
    /// - Iceberg or S3 catalog initialization
    /// - `DataFusion` catalog wrapping or setup failures
    pub async fn register_catalogs(&self) -> Result<()> {
        // Internal catalogs
        let mut catalogs = self.internal_catalogs().await?;

        // S3 tables catalogs
        catalogs.extend(self.external_catalogs().await?);

        for catalog in catalogs {
            self.catalogs
                .insert(catalog.name.clone(), Arc::new(catalog));
        }
        Ok(())
    }

    pub async fn internal_catalogs(&self) -> Result<Vec<CachingCatalog>> {
        self.metastore
            .iter_databases()
            .collect()
            .await
            .map_err(|e| Error::Core { source: e })?
            .into_iter()
            .map(|db| {
                let iceberg_catalog =
                    EmbucketIcebergCatalog::new(self.metastore.clone(), db.ident.clone())
                        .context(MetastoreSnafu)?;
                let catalog: Arc<dyn CatalogProvider> = Arc::new(EmbucketCatalog {
                    database: db.ident.clone(),
                    metastore: self.metastore.clone(),
                    iceberg_catalog: Arc::new(iceberg_catalog),
                });
                Ok(CachingCatalog {
                    catalog,
                    schemas_cache: DashMap::default(),
                    should_refresh: true,
                    name: db.ident.clone(),
                })
            })
            .collect()
    }

    pub async fn external_catalogs(&self) -> Result<Vec<CachingCatalog>> {
        let volumes = self
            .metastore
            .iter_volumes()
            .collect()
            .await
            .map_err(|e| Error::Core { source: e })?
            .into_iter()
            .filter_map(|v| match v.volume.clone() {
                MetastoreVolumeType::S3Tables(s3) => Some(s3),
                _ => None,
            })
            .collect::<Vec<_>>();

        if volumes.is_empty() {
            return Ok(vec![]);
        }

        let mut catalogs = Vec::with_capacity(volumes.len());
        for volume in volumes {
            let (ak, sk, token) = match volume.credentials {
                AwsCredentials::AccessKey(ref creds) => (
                    Some(creds.aws_access_key_id.clone()),
                    Some(creds.aws_secret_access_key.clone()),
                    None,
                ),
                AwsCredentials::Token(ref t) => (None, None, Some(t.clone())),
            };
            let creds =
                Credentials::from_keys(ak.unwrap_or_default(), sk.unwrap_or_default(), token);
            let config = SdkConfig::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(SharedCredentialsProvider::new(creds))
                .region(Region::new(volume.region.clone()))
                .build();
            let catalog = S3TablesCatalog::new(
                &config,
                volume.arn.as_str(),
                ObjectStoreBuilder::S3(volume.s3_builder()),
            )
            .context(S3TablesSnafu)?;

            let catalog = DataFusionIcebergCatalog::new(Arc::new(catalog), None)
                .await
                .context(DataFusionSnafu)?;
            catalogs.push(CachingCatalog {
                catalog: Arc::new(catalog),
                schemas_cache: DashMap::new(),
                should_refresh: false,
                name: volume.name,
            });
        }
        Ok(catalogs)
    }

    #[allow(clippy::as_conversions, clippy::too_many_lines)]
    pub async fn refresh(&self) -> Result<()> {
        for catalog in self.catalogs.iter_mut() {
            if catalog.should_refresh {
                let schemas = catalog.schema_names();
                for schema in schemas.clone() {
                    if let Some(schema_provider) = catalog.catalog.schema(&schema) {
                        let schema = CachingSchema {
                            schema: schema_provider,
                            tables_cache: DashMap::default(),
                            name: schema.to_string(),
                        };
                        let tables = schema.schema.table_names();
                        for table in tables {
                            if let Some(table_provider) =
                                schema.schema.table(&table).await.context(DataFusionSnafu)?
                            {
                                schema.tables_cache.insert(
                                    table.clone(),
                                    Arc::new(CachingTable::new_with_schema(
                                        table,
                                        table_provider.schema(),
                                        Arc::clone(&table_provider),
                                    )),
                                );
                            }
                        }
                        catalog
                            .schemas_cache
                            .insert(schema.name.clone(), Arc::new(schema));
                    }
                }
                // Cleanup removed schemas from the cache
                for schema in &catalog.schemas_cache {
                    if !schemas.contains(&schema.key().to_string()) {
                        catalog.schemas_cache.remove(schema.key());
                    }
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for EmbucketCatalogList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbucketCatalogList").finish()
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

impl ObjectStoreRegistry for EmbucketCatalogList {
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

impl CatalogProviderList for EmbucketCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let catalog = CachingCatalog {
            catalog,
            schemas_cache: DashMap::default(),
            should_refresh: false,
            name,
        };
        self.catalogs
            .insert(catalog.name.clone(), Arc::new(catalog))
            .map(|arc| {
                let catalog: Arc<dyn CatalogProvider> = arc;
                catalog
            })
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    #[allow(clippy::as_conversions)]
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs
            .get(name)
            .map(|c| Arc::clone(c.value()) as Arc<dyn CatalogProvider>)
    }
}
