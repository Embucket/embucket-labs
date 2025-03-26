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

use crate::execution::{query::IceBucketQueryContext, session::IceBucketUserSession};
use icebucket_metastore::{
    IceBucketDatabase, IceBucketSchema, IceBucketSchemaIdent, IceBucketVolume, Metastore,
    SlateDBMetastore,
};

static TABLE_SETUP: &str = include_str!(r"./queries/table_setup.sql");

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn create_df_session() -> Arc<IceBucketUserSession> {
    let metastore = SlateDBMetastore::new_in_memory().await;
    metastore
        .create_volume(
            &"test_volume".to_string(),
            IceBucketVolume::new(
                "test_volume".to_string(),
                icebucket_metastore::IceBucketVolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"icebucket".to_string(),
            IceBucketDatabase {
                ident: "icebucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = IceBucketSchemaIdent {
        database: "icebucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            IceBucketSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");

    let user_session = Arc::new(
        IceBucketUserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );

    for query in TABLE_SETUP.split(';') {
        if !query.is_empty() {
            let query = user_session.query(query, IceBucketQueryContext::default());
            query.execute().await.unwrap();
            //ctx.sql(query).await.unwrap().collect().await.unwrap();
        }
    }
    user_session
}

pub mod macros {
    macro_rules! test_query {
        ($test_fn_name:ident, $query:expr) => {
            paste::paste! {
                #[tokio::test]
                async fn [< query_ $test_fn_name >]() {
                    let ctx = crate::tests::utils::create_df_session().await;

                    let query = ctx.query($query, crate::execution::query::IceBucketQueryContext::default());
                    let statement = query.parse_query().unwrap();
                    let plan = query.plan().await;
                    //TODO: add our plan processing also
                    let df = match &plan {
                        Ok(_plan) => {
                            match query.execute().await {
                                Ok(record_batches) => {
                                    Ok(datafusion::arrow::util::pretty::pretty_format_batches(&record_batches).unwrap().to_string())
                                },
                                Err(e) => Err(format!("Error: {e}"))
                            }
                        },
                        Err(e) => Err(format!("Error: {e}"))
                    };
                    insta::with_settings!({
                        description => stringify!($query),
                        omit_expression => true,
                        prepend_module_to_snapshot => false
                    }, {
                        let plan = plan.map(|plan| plan.to_string().split("\n").map(|s| s.to_string()).collect::<Vec<String>>());
                        let df = df.map(|df| df.split("\n").map(|s| s.to_string()).collect::<Vec<String>>());
                        insta::assert_debug_snapshot!((statement, plan, df));
                    })
                }
            }
        }
    }

    pub(crate) use test_query;
}
