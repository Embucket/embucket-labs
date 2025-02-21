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

use catalog::service::Catalog;
use control_plane::service::ControlService;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct AppState {
    pub control_svc: Arc<dyn ControlService + Send + Sync>,
    pub catalog_svc: Arc<dyn Catalog + Send + Sync>,
    pub dbt_sessions: Arc<Mutex<HashMap<String, String>>>,
}

impl AppState {
    // You can add helper methods for state initialization if needed
    pub fn new(
        control_svc: Arc<dyn ControlService + Send + Sync>,
        catalog_repo: Arc<dyn Catalog + Send + Sync>,
    ) -> Self {
        Self {
            control_svc,
            catalog_svc: catalog_repo,
            dbt_sessions: Arc::new(Mutex::default()),
        }
    }
}
