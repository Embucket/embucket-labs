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

use arrow::datatypes::{DataType, SchemaRef};
use std::{collections::HashMap, sync::Arc};

use datafusion::common::file_options::file_type::FileType;
use datafusion::common::plan_datafusion_err;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::datasource::default_table_source::provider_as_source;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::var_provider::is_system_variables;
use datafusion::logical_expr::WindowUDF;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::prelude::*;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use datafusion::variable::VarType;

pub struct CustomContextProvider<'a> {
    pub(crate) state: &'a SessionState,
    pub(crate) tables: HashMap<String, Arc<dyn TableSource>>,
}

impl<'a> ContextProvider for CustomContextProvider<'a> {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let catalog = self.state.config_options().catalog.clone();
        let name = name.resolve(&catalog.default_catalog, &catalog.default_schema);
        println!("Table name: {:?}", name);
        println!("Table name: {:?}, to_string {}", name, name.to_string());
        println!("Tables: {:?}", self.tables.keys());
        self.tables
            .get(&name.to_string())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table '{name}' not found"))
    }

    fn get_file_type(&self, ext: &str) -> Result<Arc<dyn FileType>> {
        self.state
            .get_file_format_factory(ext)
            .ok_or(plan_datafusion_err!(
                "There is no registered file format with ext {ext}"
            ))
            .map(|file_type| {
                datafusion::datasource::file_format::format_as_file_type(file_type.clone())
            })
    }

    fn get_table_function_source(
        &self,
        name: &str,
        args: Vec<Expr>,
    ) -> Result<Arc<dyn TableSource>> {
        let tbl_func = self
            .state
            .table_functions()
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table function '{name}' not found"))?;
        let provider = tbl_func.create_table_provider(&args)?;

        Ok(provider_as_source(provider))
    }

    /// Create a new CTE work table for a recursive CTE logical plan
    /// This table will be used in conjunction with a Worktable physical plan
    /// to read and write each iteration of a recursive CTE
    fn create_cte_work_table(&self, name: &str, schema: SchemaRef) -> Result<Arc<dyn TableSource>> {
        let table = Arc::new(datafusion::datasource::cte_worktable::CteWorkTable::new(
            name, schema,
        ));
        Ok(provider_as_source(table))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.state
            .execution_props()
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}
