use std::any::Any;
use std::sync::{Arc, Weak};

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};

use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::common::plan_datafusion_err;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;

use crate::datafusion::data_catalog::extended_mirror::ExtendedMirror;
use crate::datafusion::data_catalog::schema::MultiSchemaProvider;
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, error::DataFusionError};
use datafusion_iceberg::catalog::mirror::Mirror;
use datafusion_iceberg::catalog::schema::IcebergSchema;
use dirs::home_dir;
use futures::{executor::LocalPool, task::LocalSpawnExt};
use iceberg_rust::object_store::store::IcebergStore;
use iceberg_rust::spec::{tabular::TabularMetadata, view_metadata::REF_PREFIX};
use iceberg_rust::{
    catalog::{
        create::{CreateMaterializedView, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog,
    },
    error::Error as IcebergError,
    object_store::Bucket,
    spec::table_metadata::new_metadata_location,
};
use parking_lot::RwLock;
use std::collections::HashSet;

#[derive(Debug)]
pub struct IcehutCatalogProvider {
    catalog: Arc<ExtendedMirror>,
}

impl IcehutCatalogProvider {
    pub async fn new(catalog: Arc<dyn Catalog>, branch: Option<&str>) -> Result<Self> {
        Ok(IcehutCatalogProvider {
            catalog: Arc::new(ExtendedMirror::new(catalog, branch.map(ToOwned::to_owned)).await?),
        })
    }

    pub fn new_sync(catalog: Arc<dyn Catalog>, branch: Option<&str>) -> Self {
        IcehutCatalogProvider {
            catalog: Arc::new(ExtendedMirror::new_sync(
                catalog,
                branch.map(ToOwned::to_owned),
            )),
        }
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.catalog()
    }
}

impl CatalogProvider for IcehutCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        let namespaces = self.catalog.schema_names(None);
        match namespaces {
            Err(_) => vec![],
            Ok(namespaces) => namespaces.into_iter().map(|x| x.to_string()).collect(),
        }
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(MultiSchemaProvider::new(
            Namespace::try_new(
                &name
                    .split('.')
                    .map(|z| z.to_owned())
                    .collect::<Vec<String>>(),
            )
            .ok()?,
            Arc::clone(&self.catalog),
        )) as Arc<dyn SchemaProvider>)
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
    }
}

// impl IcehutCatalogProvider {
//     pub fn new(iceberg_catalog: IcebergCatalog, memory_catalog: MemoryCatalogProvider) -> Self {
//         Self {
//             iceberg_catalog,
//             memory_catalog,
//         }
//     }
//
// }
//
// impl CatalogProvider for IcehutCatalogProvider {
//     fn as_any(&self) -> &dyn Any {
//         self.iceberg_catalog.as_any()
//     }
//
//     fn schema_names(&self) -> Vec<String> {
//         self.iceberg_catalog.schema_names()
//     }
//
//     fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
//         self.iceberg_catalog.schema(name)
//     }
//
//     fn register_schema(
//         &self,
//         name: &str,
//         schema: Arc<dyn SchemaProvider>,
//     ) -> Result<Option<Arc<dyn SchemaProvider>>> {
//         self.iceberg_catalog.register_schema(name, schema)
//     }
//
//     fn deregister_schema(&self, name: &str, cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
//         // Add cascade logic for removing non-empty schemas if needed
//         self.iceberg_catalog.deregister_schema(name, cascade)
//     }
//
//
// }
