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
use crate::http::ui::{
    queries::models::QueryCreateResponse,
    worksheets::models::{WorksheetCreatePayload, WorksheetCreateResponse},
};
use chrono::{TimeZone, Utc};
use icebucket_metastore::{
    IceBucketDatabase, IceBucketSchema, IceBucketSchemaIdent, IceBucketVolume,
};
use serde_json::json;

// Replace dynamic time data by some stub data
fn get_patched_query_response(query_resp: &str) -> String {
    let stub_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let mut query_resp: QueryCreateResponse =
        serde_json::from_str(query_resp).expect("Failed to parse QueryCreateResponse");
    query_resp.data.start_time = stub_time;
    query_resp.data.end_time = stub_time;
    query_resp.data.duration_ms = 100;
    serde_json::to_string(&query_resp)
        .expect("Failed to serialize QueryCreateResponse back to string")
}

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
        .post(format!("http://{addr}/ui/queries?worksheet_id={wid}"))
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
        .post(format!("http://{addr}/ui/queries?worksheet_id={wid}"))
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
        insta::assert_debug_snapshot!((
            get_patched_query_response(query1.as_str()),
            get_patched_query_response(query2.as_str()),
        ));
    });
}
