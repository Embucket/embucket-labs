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

use crate::datafusion::type_planner::CustomTypePlanner;
use datafusion::common::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_iceberg::planner::IcebergQueryPlanner;
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
pub struct Session {
    pub ctx: SessionContext,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    #[must_use]
    pub fn new() -> Self {
        let sql_parser_dialect =
            env::var("SQL_PARSER_DIALECT").unwrap_or_else(|_| "snowflake".to_string());
        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_option_extension(SessionParams::default())
                    .with_information_schema(true)
                    .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect),
            )
            .with_default_features()
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .with_type_planner(Arc::new(CustomTypePlanner {}))
            .build();
        let ctx = SessionContext::new_with_state(state);
        Self { ctx }
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
