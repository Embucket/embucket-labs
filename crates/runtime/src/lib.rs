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

use config::RuntimeConfig;
use embucket_history::store::SlateDBWorksheetsStore;
use embucket_metastore::SlateDBMetastore;
use embucket_utils::Db;
use http::web_assets::run_web_assets_server;
use http::{make_app, run_app};
use object_store::{path::Path, ObjectStore};
use slatedb::{config::DbOptions, db::Db as SlateDb};

pub mod config;
pub mod execution;
pub mod http;

#[cfg(test)]
pub(crate) mod tests;

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_binary(
    state_store: Arc<dyn ObjectStore>,
    config: RuntimeConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = {
        let options = DbOptions::default();
        Db::new(Arc::new(
            SlateDb::open_with_opts(
                Path::from(config.db.slatedb_prefix.clone()),
                options,
                state_store.clone(),
            )
            .await
            .map_err(Box::new)?,
        ))
    };

    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history = Arc::new(SlateDBWorksheetsStore::new(db));
    let app = make_app(metastore, history, &config.web)?;

    let _ = run_web_assets_server(&config.web_assets).await?;

    run_app(app, &config.web).await
}
