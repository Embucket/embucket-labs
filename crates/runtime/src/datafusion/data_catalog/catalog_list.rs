use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::{any::Any, sync::Arc};
use std::sync::{RwLock, Weak};
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::datasource::stream;
use datafusion::execution::SessionState;
pub use crate::data_catalog::schema::MultiSchemaProvider;
use datafusion_common::not_impl_err;
use datafusion_common::Result;
use crate::data_catalog::catalog::IcehutCatalogProvider;
/*
#[derive(Debug)]
pub struct IcehutCatalogProviderList {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>, 
    state: Weak<RwLock<SessionState>>,
}

impl IcehutCatalogProviderList {
    pub async fn new(
        catalog_list: Arc<dyn CatalogProviderList>,
        state: Weak<RwLock<SessionState>>,
    ) -> Self {
        let catalogs = catalog_list.list_catalogs().await;

        let map = stream::iter(catalogs.into_iter())
            .then(|x| {
                let catalog_list = catalog_list.clone();
                async move {
                    let catalog = catalog_list.catalog(&x)?;
                    Some((
                        x,
                        Arc::new(IcehutCatalogProvider::new(catalog).await.ok()?)
                            as Arc<dyn CatalogProvider>,
                    ))
                }
            })
            .filter_map(|x| async move { x })
            .collect::<HashMap<_, _>>()
            .await;

        Ok(IcehutCatalogProviderList {
            catalogs: map,
            state,
        })
    }
}

impl CatalogProviderList for IcehutCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = self.catalogs.as_any().catalogs();
        catalogs.insert(name, catalog);
        Some(catalog)
    }

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}*/