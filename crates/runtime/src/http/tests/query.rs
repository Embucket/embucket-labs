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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::tests::run_icebucket_test_server;
// for `collect`
use crate::http::ui::worksheets::models::{WorksheetCreatePayload, WorksheetCreateResponse};
use icebucket_metastore::{
    IceBucketDatabase, IceBucketSchema, IceBucketSchemaIdent, IceBucketVolume,
};
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_parallel_queries() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();
    let client2 = reqwest::Client::new();

    let vol = IceBucketVolume {
        ident: "test_volume".to_string(),
        volume: icebucket_metastore::IceBucketVolumeType::Memory,
    };

    let _create_volume = client
        .post(format!("http://{addr}/v1/metastore/volumes"))
        .header("Content-Type", "application/json")
        .body(json!(vol).to_string())
        .send()
        .await
        .expect("failed to create volume")
        .error_for_status_ref()
        .expect("Create volume wasn't 200");

    let db = IceBucketDatabase {
        ident: "benchmark".to_string(),
        volume: "test_volume".to_string(),
        properties: None,
    };

    let _create_db = client
        .post(format!("http://{addr}/v1/metastore/databases"))
        .header("Content-Type", "application/json")
        .body(json!(db).to_string())
        .send()
        .await
        .expect("failed to create database")
        .error_for_status_ref()
        .expect("Create database wasn't 200");

    let schema = IceBucketSchema {
        ident: IceBucketSchemaIdent {
            database: "benchmark".to_string(),
            schema: "public".to_string(),
        },
        properties: None,
    };

    let _create_schema = client
        .post(format!(
            "http://{addr}/v1/metastore/databases/benchmark/schemas"
        ))
        .header("Content-Type", "application/json")
        .body(json!(schema).to_string())
        .send()
        .await
        .expect("failed to create schema")
        .error_for_status_ref()
        .expect("Create schema wasn't 200");

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

    // Create worksheet before running queries
    let create_worksheet = client
        .post(format!("http://{addr}/ui/worksheets"))
        .header("Content-Type", "application/json")
        .body(
            json!(WorksheetCreatePayload {
                name: String::new(),
                content: String::new(),
            })
            .to_string(),
        )
        .send()
        .await
        .expect("failed to create worksheet");

    let wid = create_worksheet
        .json::<WorksheetCreateResponse>()
        .await
        .unwrap()
        .data
        .id;

    let query1 = client
        .post(format!("http://{addr}/ui/worksheets/{wid}/queries"))
        .header("Content-Type", "application/json")
        .body(
            json!({
                "query": create_query,
                "context": {}
            })
            .to_string(),
        )
        .send()
        .await
        .expect("failed to create query");

    query1
        .error_for_status_ref()
        .expect("Create query wasn't 200");

    let query1 = query1.text().await.expect("Failed to get query response");

    let mut insert_query = "INSERT INTO benchmark.public.hits VALUES ".to_string();
    for i in 0..100 {
        insert_query.push_str(&format!("({i}, 1, 'test', 1, 1, 1, 1, 1),"));
    }
    insert_query.push_str("(200, 1, 'test', 1, 1, 1, 1, 1);");

    let query2 = client2
        .post(format!("http://{addr}/ui/worksheets/{wid}/queries"))
        .header("Content-Type", "application/json")
        .body(
            json!({
                "query": insert_query,
                "context": {}
            })
            .to_string(),
        )
        .send()
        .await
        .expect("failed to run insert query");

    query2
        .error_for_status_ref()
        .expect("Insert query wasn't 200");
    let query2 = query2.text().await.expect("Failed to get query response");

    insta::with_settings!({
    filters => vec![
        (r"\d+{5,}", "99999"),
    ]}, {
        insta::assert_debug_snapshot!((query1, query2,));
    });
}
