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
use crate::http::ui::queries::models::{
    Column, QueriesResponse, QueryCreatePayload, QueryCreateResponse, ResultSet,
};
use crate::http::ui::tests::common::req;
use crate::http::ui::worksheets::models::{WorksheetCreatePayload, WorksheetResponse};
use crate::tests::run_test_server;
use embucket_history::QueryStatus;
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_queries_no_worksheet() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(0),
            query: "SELECT 1".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries?worksheetId={}", 0),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let history_resp = res.json::<QueriesResponse>().await.unwrap();
    assert_eq!(history_resp.items.len(), 1);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_queries() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets"),
        json!(WorksheetCreatePayload {
            name: String::new(),
            content: String::new(),
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
        &format!("http://{addr}/ui/queries?worksheetId={}", worksheet.id),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::METHOD_NOT_ALLOWED, res.status());

    // Bad key - not found
    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::BAD_REQUEST, res.status());

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT 1, 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");
    //println!("{:?}", res.bytes().await);

    let query_run_resp = res.json::<QueryCreateResponse>().await.unwrap();
    // println!("query_run_resp: {query_run_resp:?}");
    assert_eq!(
        query_run_resp.data.result,
        ResultSet {
            columns: vec![
                Column {
                    name: "Int64(1)".to_string(),
                    r#type: "fixed".to_string(),
                },
                Column {
                    name: "Int64(2)".to_string(),
                    r#type: "fixed".to_string(),
                }
            ],
            rows: serde_json::from_str("[[1,2]]").unwrap()
        }
    );
    // assert_eq!(query_run_resp.result, "[{\"Int64(1)\":1}]");

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let query_run_resp2 = res.json::<QueryCreateResponse>().await.unwrap();
    // println!("query_run_resp2: {query_run_resp2:?}");
    assert_eq!(
        query_run_resp2.data.result,
        ResultSet {
            columns: vec![Column {
                name: "Int64(2)".to_string(),
                r#type: "fixed".to_string(),
            }],
            rows: serde_json::from_str("[[2]]").unwrap()
        }
    );
    // assert_eq!(query_run_resp2.result, "[{\"Int64(2)\":2}]");

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
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
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT foo".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");
    // println!("err resp: {:?}", res.text().await.expect("Can't get response text"));
    assert_eq!(http::StatusCode::UNPROCESSABLE_ENTITY, res.status());
    let err = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(err.status_code, http::StatusCode::UNPROCESSABLE_ENTITY);

    // get all=4
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries?worksheetId={}", worksheet.id),
        String::new(),
    )
    .await
    .expect("Error getting queries");
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let history_resp = res.json::<QueriesResponse>().await.unwrap();
    assert_eq!(history_resp.items.len(), 4);

    // get 2
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/queries?worksheetId={}&limit=2",
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
    assert_eq!(history_resp.items[0].result, query_run_resp.data.result);
    assert_eq!(history_resp.items[1].status, QueryStatus::Ok);
    assert_eq!(history_resp.items[1].result, query_run_resp2.data.result);

    // get rest
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/queries?worksheetId={}&cursor={}",
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
