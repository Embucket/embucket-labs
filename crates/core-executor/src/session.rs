//use super::datafusion::functions::geospatial::register_udfs as register_geo_udfs;
use super::datafusion::functions::register_udfs;
use super::datafusion::type_planner::CustomTypePlanner;
use super::dedicated_executor::DedicatedExecutor;
use super::error::{
    self as ex_error, ExecutionError, ExecutionResult, RefreshCatalogListSnafu,
    RegisterCatalogSnafu,
};
use super::query::{QueryContext, UserQuery};
use crate::datafusion::analyzer::IcebergTypesAnalyzer;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use core_metastore::error::MetastoreError;
use core_metastore::{AwsCredentials, Metastore, VolumeType as MetastoreVolumeType};
use core_utils::scan_iterator::ScanIterator;
use datafusion::catalog::CatalogProvider;
use datafusion::common::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_functions_json::register_all as register_json_udfs;
use datafusion_iceberg::catalog::catalog::IcebergCatalog as DataFusionIcebergCatalog;
use datafusion_iceberg::planner::IcebergQueryPlanner;
use df_builtins::register_udafs;
use df_catalog::catalog_list::{DEFAULT_CATALOG, EmbucketCatalogList};
// TODO: We need to fix this after geodatafusion is updated to datafusion 47
//use geodatafusion::udf::native::register_native as register_geo_native;
use crate::datafusion::physical_optimizer::physical_optimizer_rules;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_s3tables_catalog::S3TablesCatalog;
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

pub struct UserSession {
    pub metastore: Arc<dyn Metastore>,
    pub ctx: SessionContext,
    pub ident_normalizer: IdentNormalizer,
    pub executor: DedicatedExecutor,
}

impl UserSession {
    pub async fn new(metastore: Arc<dyn Metastore>) -> ExecutionResult<Self> {
        let sql_parser_dialect =
            env::var("SQL_PARSER_DIALECT").unwrap_or_else(|_| "snowflake".to_string());

        let catalog_list_impl = Arc::new(EmbucketCatalogList::new(metastore.clone()));

        let runtime_config = RuntimeEnvBuilder::new()
            .with_object_store_registry(catalog_list_impl.clone())
            .build()
            .context(ex_error::DataFusionSnafu)?;

        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_option_extension(SessionParams::default())
                    .with_information_schema(true)
                    // Cannot create catalog (database) automatic since it requires default volume
                    .with_create_default_catalog_and_schema(false)
                    .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect)
                    .set_str("datafusion.catalog.default_catalog", DEFAULT_CATALOG),
            )
            .with_default_features()
            .with_runtime_env(Arc::new(runtime_config))
            .with_catalog_list(catalog_list_impl.clone())
            .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
            .with_type_planner(Arc::new(CustomTypePlanner {}))
            .with_analyzer_rule(Arc::new(IcebergTypesAnalyzer {}))
            .with_physical_optimizer_rules(physical_optimizer_rules())
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        register_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        register_udafs(&mut ctx).context(ex_error::RegisterUDAFSnafu)?;
        register_json_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        //register_geo_native(&ctx);
        //register_geo_udfs(&ctx);

        catalog_list_impl
            .register_catalogs()
            .await
            .context(RegisterCatalogSnafu)?;
        catalog_list_impl
            .refresh()
            .await
            .context(RefreshCatalogListSnafu)?;
        catalog_list_impl.start_refresh_internal_catalogs_task(10);
        let enable_ident_normalization = ctx.enable_ident_normalization();
        let session = Self {
            metastore,
            ctx,
            ident_normalizer: IdentNormalizer::new(enable_ident_normalization),
            executor: DedicatedExecutor::builder().build(),
        };
        session.register_external_catalogs().await?;
        Ok(session)
    }

    #[allow(clippy::as_conversions)]
    pub async fn register_external_catalogs(&self) -> ExecutionResult<()> {
        let volumes = self
            .metastore
            .iter_volumes()
            .collect()
            .await
            .map_err(|e| ExecutionError::Metastore {
                source: MetastoreError::UtilSlateDB { source: e },
            })?
            .into_iter()
            .filter_map(|volume| {
                if let MetastoreVolumeType::S3Tables(s3_volume) = volume.volume.clone() {
                    Some(s3_volume)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if volumes.is_empty() {
            return Ok(());
        }
        for volume in volumes {
            let (ak, sk, token) = match volume.credentials {
                AwsCredentials::AccessKey(ref creds) => (
                    Some(creds.aws_access_key_id.clone()),
                    Some(creds.aws_secret_access_key.clone()),
                    None,
                ),
                AwsCredentials::Token(ref token) => (None, None, Some(token.clone())),
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
            .context(ex_error::S3TablesSnafu)?;

            let catalog = DataFusionIcebergCatalog::new(Arc::new(catalog), None)
                .await
                .context(ex_error::DataFusionSnafu)?;
            let catalog_provider = Arc::new(catalog) as Arc<dyn CatalogProvider>;

            self.ctx.register_catalog(volume.name, catalog_provider);
        }
        Ok(())
    }

    pub fn query<S>(self: &Arc<Self>, query: S, query_context: QueryContext) -> UserQuery
    where
        S: Into<String>,
    {
        UserQuery::new(self.clone(), query.into(), query_context)
    }

    pub fn set_session_variable(
        &self,
        set: bool,
        params: HashMap<String, String>,
    ) -> ExecutionResult<()> {
        let state = self.ctx.state_ref();
        let mut write = state.write();
        let config = write
            .config_mut()
            .options_mut()
            .extensions
            .get_mut::<SessionParams>();
        if let Some(cfg) = config {
            if set {
                cfg.set_properties(params)
                    .context(ex_error::DataFusionSnafu)?;
            } else {
                cfg.remove_properties(params)
                    .context(ex_error::DataFusionSnafu)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn get_session_variable(&self, variable: &str) -> Option<String> {
        let state = self.ctx.state();
        let config = state.config().options().extensions.get::<SessionParams>();
        if let Some(cfg) = config {
            return cfg.properties.get(variable).cloned();
        }
        None
    }
}

#[derive(Default, Debug, Clone)]
pub struct SessionParams {
    pub properties: HashMap<String, String>,
}

impl SessionParams {
    pub fn set_properties(&mut self, properties: HashMap<String, String>) -> DFResult<()> {
        for (key, value) in properties {
            self.properties
                .insert(format!("session_params.{key}"), value);
        }
        Ok(())
    }

    pub fn remove_properties(&mut self, properties: HashMap<String, String>) -> DFResult<()> {
        for (key, ..) in properties {
            self.properties.remove(&key);
        }
        Ok(())
    }
}

impl ConfigExtension for SessionParams {
    const PREFIX: &'static str = "session_params";
}

impl ExtensionOptions for SessionParams {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        self.properties.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        self.properties
            .iter()
            .map(|(k, v)| ConfigEntry {
                key: k.into(),
                value: Some(v.into()),
                description: "",
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use core_metastore::{
        Database as MetastoreDatabase, Metastore, Schema as MetastoreSchema,
        SchemaIdent as MetastoreSchemaIdent, SlateDBMetastore, Volume as MetastoreVolume,
    };

    use crate::{query::QueryContext, session::UserSession};

    #[tokio::test]
    #[allow(clippy::expect_used, clippy::manual_let_else, clippy::too_many_lines)]
    async fn test_create_table_and_insert() {
        let metastore = SlateDBMetastore::new_in_memory().await;
        metastore
            .create_volume(
                &"test_volume".to_string(),
                MetastoreVolume::new(
                    "test_volume".to_string(),
                    core_metastore::VolumeType::Memory,
                ),
            )
            .await
            .expect("Failed to create volume");
        metastore
            .create_database(
                &"benchmark".to_string(),
                MetastoreDatabase {
                    ident: "benchmark".to_string(),
                    properties: None,
                    volume: "test_volume".to_string(),
                },
            )
            .await
            .expect("Failed to create database");
        let schema_ident = MetastoreSchemaIdent {
            database: "benchmark".to_string(),
            schema: "public".to_string(),
        };
        metastore
            .create_schema(
                &schema_ident.clone(),
                MetastoreSchema {
                    ident: schema_ident,
                    properties: None,
                },
            )
            .await
            .expect("Failed to create schema");
        let session = Arc::new(
            UserSession::new(metastore)
                .await
                .expect("Failed to create user session"),
        );
        let create_query = r"
        CREATE TABLE benchmark.public.hits
        (
            WatchID BIGINT NOT NULL,
            JavaEnable INTEGER NOT NULL,
            Title TEXT NOT NULL,
            GoodEvent INTEGER NOT NULL,
            EventTime BIGINT NOT NULL,
            EventDate INTEGER NOT NULL,
            CounterID INTEGER NOT NULL,
            ClientIP INTEGER NOT NULL,
            PRIMARY KEY (CounterID, EventDate, EventTime, WatchID)
        );
    ";
        let mut query1 = session.query(create_query, QueryContext::default());

        let statement = query1.parse_query().expect("Failed to parse query");
        let result = query1.execute().await.expect("Failed to execute query");

        let all_query = session
            .query("SHOW TABLES", QueryContext::default())
            .execute()
            .await
            .expect("Failed to execute query");

        let insert_query = session
            .query(
                "INSERT INTO benchmark.public.hits VALUES (1, 1, 'test', 1, 1, 1, 1, 1)",
                QueryContext::default(),
            )
            .execute()
            .await
            .expect("Failed to execute query");

        let select_query = session
            .query(
                "SELECT * FROM benchmark.public.hits",
                QueryContext::default(),
            )
            .execute()
            .await
            .expect("Failed to execute query");

        insta::assert_debug_snapshot!((statement, result, all_query, insert_query, select_query));
    }
}
