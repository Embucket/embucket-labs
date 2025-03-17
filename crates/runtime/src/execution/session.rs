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

use super::catalog::IceBucketDFMetastore;
use super::datafusion::functions::geospatial::register_udfs as register_geo_udfs;
use super::datafusion::functions::register_udfs;
use super::datafusion::type_planner::IceBucketTypePlanner;
use super::dedicated_executor::DedicatedExecutor;
use super::query::{IceBucketQuery, IceBucketQueryContext};
use datafusion::common::error::Result as DFResult;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::functions::register_all;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_iceberg::planner::IcebergQueryPlanner;
use geodatafusion::udf::native::register_native as register_geo_native;
use icebucket_metastore::Metastore;
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use super::error::{self as ex_error, ExecutionResult};

pub struct IceBucketUserSession {
    pub metastore: Arc<dyn Metastore>,
    pub ctx: SessionContext,
    pub ident_normalizer: IdentNormalizer,
    pub executor: DedicatedExecutor,
}

impl IceBucketUserSession {
    pub async fn new(metastore: Arc<dyn Metastore>) -> ExecutionResult<Self> {
        let sql_parser_dialect =
            env::var("SQL_PARSER_DIALECT").unwrap_or_else(|_| "snowflake".to_string());

        let catalog_list_impl = Arc::new(IceBucketDFMetastore::new(metastore.clone()));

        let runtime_config = RuntimeEnvBuilder::new()
            .with_object_store_registry(catalog_list_impl.clone())
            .build()
            .context(ex_error::DataFusionSnafu)?;

        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_option_extension(IceBucketSessionParams::default())
                    .with_information_schema(true)
                    .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect)
                    .set_str("datafusion.catalog.default_catalog", "icebucket"),
            )
            .with_default_features()
            .with_runtime_env(Arc::new(runtime_config))
            .with_catalog_list(catalog_list_impl.clone())
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .with_type_planner(Arc::new(IceBucketTypePlanner {}))
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        register_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        register_all(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        register_geo_native(&ctx);
        register_geo_udfs(&ctx);

        catalog_list_impl.refresh(&ctx).await?;

        let enable_ident_normalization = ctx.enable_ident_normalization();
        Ok(Self {
            metastore,
            ctx,
            ident_normalizer: IdentNormalizer::new(enable_ident_normalization),
            executor: DedicatedExecutor::builder().build(),
        })
    }

    pub fn query<S>(
        self: &Arc<Self>,
        query: S,
        query_context: IceBucketQueryContext,
    ) -> IceBucketQuery
    where
        S: Into<String>,
    {
        IceBucketQuery::new(self.clone(), query.into(), query_context)
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
            .get_mut::<IceBucketSessionParams>();
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
        let config = state
            .config()
            .options()
            .extensions
            .get::<IceBucketSessionParams>();
        if let Some(cfg) = config {
            return cfg.properties.get(variable).cloned();
        }
        None
    }
}

#[derive(Default, Debug, Clone)]
pub struct IceBucketSessionParams {
    pub properties: HashMap<String, String>,
}

impl IceBucketSessionParams {
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

impl ConfigExtension for IceBucketSessionParams {
    const PREFIX: &'static str = "session_params";
}

impl ExtensionOptions for IceBucketSessionParams {
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
