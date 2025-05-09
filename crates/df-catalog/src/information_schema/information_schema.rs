//! [`InformationSchemaProvider`] that implements the SQL [Information Schema] for Snowflake.
//!
//! [Information Schema]: https://docs.snowflake.com/en/sql-reference/info-schema

use crate::information_schema::columns::InformationSchemaColumns;
use crate::information_schema::config::InformationSchemaConfig;
use crate::information_schema::databases::InformationSchemaDatabases;
use crate::information_schema::df_settings::InformationSchemaDfSettings;
use crate::information_schema::parameters::InformationSchemaParameters;
use crate::information_schema::routines::InformationSchemaRoutines;
use crate::information_schema::schemata::InformationSchemata;
use crate::information_schema::tables::InformationSchemaTables;
use crate::information_schema::views::InformationSchemaViews;
use async_trait::async_trait;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{CatalogProviderList, SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_common::error::Result;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};

pub const INFORMATION_SCHEMA: &str = "information_schema";
pub const TABLES: &str = "tables";
pub const VIEWS: &str = "views";
pub const COLUMNS: &str = "columns";
pub const SCHEMATA: &str = "schemata";
pub const DATABASES: &str = "databases";

pub const DF_SETTINGS: &str = "df_settings";
pub const ROUTINES: &str = "routines";
pub const PARAMETERS: &str = "parameters";

/// All information schema tables
pub const INFORMATION_SCHEMA_TABLES: &[&str] = &[
    TABLES,
    VIEWS,
    COLUMNS,
    SCHEMATA,
    DF_SETTINGS,
    ROUTINES,
    PARAMETERS,
];

/// Implements the `information_schema` virtual schema and tables
///
/// The underlying tables in the `information_schema` are created on
/// demand. This means that if more tables are added to the underlying
/// providers, they will appear the next time the `information_schema`
/// table is queried.
#[derive(Debug)]
pub struct InformationSchemaProvider {
    config: InformationSchemaConfig,
}

impl InformationSchemaProvider {
    /// Creates a new [`InformationSchemaProvider`] for the provided `catalog_list`
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>, catalog_name: Arc<str>) -> Self {
        Self {
            config: InformationSchemaConfig {
                catalog_list,
                catalog_name,
            },
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        INFORMATION_SCHEMA_TABLES
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let config = self.config.clone();
        let table: Arc<dyn PartitionStream> = match name.to_ascii_lowercase().as_str() {
            TABLES => Arc::new(InformationSchemaTables::new(config)),
            COLUMNS => Arc::new(InformationSchemaColumns::new(config)),
            VIEWS => Arc::new(InformationSchemaViews::new(config)),
            SCHEMATA => Arc::new(InformationSchemata::new(config)),
            DATABASES => Arc::new(InformationSchemaDatabases::new(config)),
            // TODO: Check if non-Snowflake related tables are required
            DF_SETTINGS => Arc::new(InformationSchemaDfSettings::new()),
            ROUTINES => Arc::new(InformationSchemaRoutines::new()),
            PARAMETERS => Arc::new(InformationSchemaParameters::new()),
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(StreamingTable::try_new(
            Arc::clone(table.schema()),
            vec![table],
        )?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        INFORMATION_SCHEMA_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}
