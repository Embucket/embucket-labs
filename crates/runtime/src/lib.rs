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

use config::IceBucketRuntimeConfig;
use execution::{
    query::IceBucketQueryContext,
    session::IceBucketUserSession,
    utils::{records_to_csv_string, records_to_json_string, DataSerializationFormat},
};
use http::{make_icebucket_app, run_icebucket_app};
use icebucket_metastore::SlateDBMetastore;
use icebucket_utils::Db;
use object_store::{path::Path, ObjectStore};
use slatedb::{config::DbOptions, db::Db as SlateDb};

pub mod config;
pub mod execution;
pub mod http;

#[cfg(test)]
pub(crate) mod tests;

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_icebucket(
    state_store: Arc<dyn ObjectStore>,
    config: IceBucketRuntimeConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = {
        let options = DbOptions::default();
        Db::new(Arc::new(
            SlateDb::open_with_opts(
                Path::from(config.db.slatedb_prefix.clone()),
                options,
                state_store,
            )
            .await
            .map_err(Box::new)?,
        ))
    };

    let metastore = Arc::new(SlateDBMetastore::new(db));
    let app = make_icebucket_app(metastore, &config.web)?;
    run_icebucket_app(app, &config.web).await
}

#[allow(clippy::print_stdout)]
pub async fn run_icebucket_sql(
    state_store: Arc<dyn ObjectStore>,
    config: IceBucketRuntimeConfig,
    query: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = {
        let options = DbOptions::default();
        Db::new(Arc::new(
            SlateDb::open_with_opts(
                Path::from(config.db.slatedb_prefix.clone()),
                options,
                state_store,
            )
            .await
            .map_err(Box::new)?,
        ))
    };

    let metastore = Arc::new(SlateDBMetastore::new(db));
    let user_session = Arc::new(IceBucketUserSession::new(metastore.clone()).await?);
    let query = user_session.query(query, IceBucketQueryContext::default());
    let result = query.execute().await?;
    let result_string = match config.web.data_format {
        DataSerializationFormat::Json => records_to_json_string(&result)?,
        DataSerializationFormat::Csv => records_to_csv_string(&result)?,
        DataSerializationFormat::Arrow => {
            unimplemented!()
        }
    };
    println!("{result_string}");
    Ok(())
}
