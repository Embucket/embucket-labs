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

use crate::http::error::ErrorResponse;
use crate::http::ui::queries::models::{QueriesResponse, QueryPayload, QueryResponse};
use crate::http::ui::tests::common::req;
use crate::http::ui::worksheets::models::{WorksheetPayload, WorksheetResponse};
use crate::tests::run_icebucket_test_server;
use http::Method;
use icebucket_history::QueryStatus;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_queries() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets"),
        json!(WorksheetPayload {
            name: None,
            content: None,
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let worksheet = res.json::<WorksheetResponse>().await.unwrap().data;
    assert!(worksheet.id > 0);

    let res = req(
        &client,
        Method::DELETE,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::METHOD_NOT_ALLOWED, res.status());

    // Bad key - not found
    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", 0),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::BAD_REQUEST, res.status());

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        json!(QueryPayload {
            query: "SELECT 1".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let query_run_resp = res.json::<QueryResponse>().await.unwrap();
    assert_eq!(query_run_resp.result, "[{\"Int64(1)\":1}]");

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        json!(QueryPayload {
            query: "SELECT 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let query_run_resp2 = res.json::<QueryResponse>().await.unwrap();
    assert_eq!(query_run_resp2.result, "[{\"Int64(2)\":2}]");

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        json!(QueryPayload {
            query: "SELECT foo".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::UNPROCESSABLE_ENTITY, res.status());
    let err = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(err.status_code, http::StatusCode::UNPROCESSABLE_ENTITY);

    // second fail
    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        json!(QueryPayload {
            query: "SELECT foo".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::UNPROCESSABLE_ENTITY, res.status());
    let err = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(err.status_code, http::StatusCode::UNPROCESSABLE_ENTITY);

    // get 2
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/worksheets/{}/queries?limit=2",
            worksheet.id
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let history_resp = res.json::<QueriesResponse>().await.unwrap();
    assert_eq!(history_resp.items.len(), 2);
    assert_eq!(history_resp.items[0].status, QueryStatus::Ok);
    assert_eq!(history_resp.items[0].result, Some(query_run_resp.result));
    assert_eq!(history_resp.items[1].status, QueryStatus::Ok);
    assert_eq!(history_resp.items[1].result, Some(query_run_resp2.result));

    // get rest
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/worksheets/{}/queries?cursor={}",
            worksheet.id, history_resp.next_cursor
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let history_resp = res.json::<QueriesResponse>().await.unwrap();
    assert_eq!(history_resp.items.len(), 2);
    assert_eq!(history_resp.items[0].status, QueryStatus::Error);
    assert_eq!(history_resp.items[1].status, QueryStatus::Error);
}
