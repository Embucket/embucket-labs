use super::datafusion::functions::geospatial::register_udfs as register_geo_udfs;
use super::datafusion::functions::register_udfs;
use super::datafusion::type_planner::CustomTypePlanner;
use super::dedicated_executor::DedicatedExecutor;
use super::error::{self as ex_error, ExecutionResult};
use super::query::{QueryContext, UserQuery};
use crate::execution::catalog::catalog_list::{EmbucketCatalogList, DEFAULT_CATALOG};
use crate::execution::datafusion::analyzer::IcebergTypesAnalyzer;
use datafusion::common::error::Result as DFResult;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_functions_json::register_all as register_json_udfs;
use datafusion_iceberg::planner::IcebergQueryPlanner;
use embucket_metastore::Metastore;
use geodatafusion::udf::native::register_native as register_geo_native;
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
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .with_type_planner(Arc::new(CustomTypePlanner {}))
            .with_analyzer_rule(Arc::new(IcebergTypesAnalyzer {}))
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        register_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        register_json_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        register_geo_native(&ctx);
        register_geo_udfs(&ctx);

        catalog_list_impl.register_catalogs().await?;
        catalog_list_impl.refresh().await?;

        let enable_ident_normalization = ctx.enable_ident_normalization();
        let session = Self {
            metastore,
            ctx,
            ident_normalizer: IdentNormalizer::new(enable_ident_normalization),
            executor: DedicatedExecutor::builder().build(),
        };
        Ok(session)
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
