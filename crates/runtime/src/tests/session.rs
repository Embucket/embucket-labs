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

use icebucket_metastore::{
    IceBucketDatabase, IceBucketSchema, IceBucketSchemaIdent, IceBucketVolume, Metastore,
    SlateDBMetastore,
};

use crate::execution::{query::IceBucketQueryContext, session::IceBucketUserSession};

#[tokio::test]
#[allow(clippy::expect_used, clippy::manual_let_else, clippy::too_many_lines)]
async fn test_create_table_and_insert() {
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
            &"benchmark".to_string(),
            IceBucketDatabase {
                ident: "benchmark".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = IceBucketSchemaIdent {
        database: "benchmark".to_string(),
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
    let session = Arc::new(
        IceBucketUserSession::new(metastore)
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
    let query1 = session.query(create_query, IceBucketQueryContext::default());

    let statement = query1.parse_query().expect("Failed to parse query");
    let result = query1.execute().await.expect("Failed to execute query");

    let all_query = session
        .query("SHOW TABLES", IceBucketQueryContext::default())
        .execute()
        .await
        .expect("Failed to execute query");

    let insert_query = session
        .query(
            "INSERT INTO benchmark.public.hits VALUES (1, 1, 'test', 1, 1, 1, 1, 1)",
            IceBucketQueryContext::default(),
        )
        .execute()
        .await
        .expect("Failed to execute query");

    let select_query = session
        .query(
            "SELECT * FROM benchmark.public.hits",
            IceBucketQueryContext::default(),
        )
        .execute()
        .await
        .expect("Failed to execute query");

    insta::assert_debug_snapshot!((statement, result, all_query, insert_query, select_query));
}
