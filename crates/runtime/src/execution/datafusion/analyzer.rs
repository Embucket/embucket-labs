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

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::optimizer::AnalyzerRule;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{CreateCatalog, CreateCatalogSchema, DdlStatement, Extension, LogicalPlan};

#[derive(Debug)]
struct IcebergAnalyzerRule {}

impl AnalyzerRule for IcebergAnalyzerRule {
    
    fn analyze(&self, plan: LogicalPlan, _config: &datafusion::config::ConfigOptions) -> DFResult<LogicalPlan> {
        let transformed_plan = plan.transform(datafusion_iceberg::planner::iceberg_transform)?;
        Ok(transformed_plan.data)
    }
    
    fn name(&self) -> &str {
        "IcebergAnalyzerRule"
    }
}

pub struct IcebergCreateCatalog(CreateCatalog);
pub struct IcebergCreateCatalogSchema(CreateCatalogSchema);

