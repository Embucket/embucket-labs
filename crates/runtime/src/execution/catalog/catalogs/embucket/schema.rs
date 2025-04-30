use crate::execution::catalog::catalogs::embucket::block_on_with_fallback;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use embucket_metastore::{Metastore, SchemaIdent, TableIdent};
use embucket_utils::scan_iterator::ScanIterator;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::{catalog::tabular::Tabular as IcebergTabular, table::Table as IcebergTable};
use std::any::Any;
use std::sync::Arc;

pub struct EmbucketSchema {
    pub database: String,
    pub schema: String,
    pub metastore: Arc<dyn Metastore>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for EmbucketSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFSchema")
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("metastore", &"")
            .field("iceberg_catalog", &"")
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for EmbucketSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let metastore = self.metastore.clone();
        let database = self.database.clone();
        let schema = self.schema.to_string();

        block_on_with_fallback(async move {
            match metastore
                .iter_tables(&SchemaIdent::new(database, schema))
                .collect()
                .await
            {
                Ok(tables) => tables.into_iter().map(|s| s.ident.table.clone()).collect(),
                Err(_) => vec![],
            }
        })
        .unwrap_or_else(|_| vec![])
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let ident = &TableIdent::new(&self.database.clone(), &self.schema.clone(), name);
        match self.metastore.get_table(ident).await {
            Ok(Some(table)) => {
                let iceberg_table = IcebergTable::new(
                    ident.to_iceberg_ident(),
                    self.iceberg_catalog.clone(),
                    table.metadata.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let tabular = IcebergTabular::Table(iceberg_table);
                let table_provider: Arc<dyn TableProvider> =
                    Arc::new(IcebergDataFusionTable::new(tabular, None, None, None));
                Ok(Some(table_provider))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let iceberg_catalog = self.iceberg_catalog.clone();
        let database = self.database.clone();
        let schema = self.schema.clone();
        let table = name.to_string();

        block_on_with_fallback(async move {
            let ident = TableIdent::new(&database, &schema, &table);
            iceberg_catalog
                .tabular_exists(&ident.to_iceberg_ident())
                .await
                .unwrap_or(false)
        })
        .unwrap_or(false)
    }
}
