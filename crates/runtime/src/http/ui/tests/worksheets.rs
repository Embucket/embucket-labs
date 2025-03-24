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
use crate::http::ui::tests::common::req;
use crate::http::ui::worksheets::{WorksheetPayload, WorksheetResponse, WorksheetsResponse};
use crate::tests::run_icebucket_test_server;
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_worksheets() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();

    let res = req(
        &client,
        Method::DELETE,
        &format!("http://{addr}/ui/worksheets"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::METHOD_NOT_ALLOWED, res.status());

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
    let worksheet1 = res.json::<WorksheetResponse>().await.unwrap().data;
    assert!(worksheet1.id > 0);
    assert!(worksheet1.name.is_none());
    assert!(worksheet1.content.is_none());

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets"),
        json!(WorksheetPayload {
            name: None,
            content: Some("select 1;".to_string()),
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let worksheet2 = res.json::<WorksheetResponse>().await.unwrap().data;
    assert!(worksheet2.id > 0);
    assert!(worksheet2.name.is_none());
    assert!(worksheet2.content.is_some());

    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/worksheets"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    // println!("{:?}", res.bytes().await);
    let worksheets = res.json::<WorksheetsResponse>().await.unwrap().items;
    assert_eq!(worksheets.len(), 2);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_worksheets_ops() {
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

    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/worksheets/{}", 0),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::NOT_FOUND, res.status());
    let error = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(http::StatusCode::NOT_FOUND, error.status_code);

    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/worksheets/{}", worksheet.id),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let patch_payload = WorksheetPayload {
        name: Some("test".to_string()),
        content: Some("select 1".to_string()),
    };
    let res = req(
        &client,
        Method::PATCH,
        &format!("http://{addr}/ui/worksheets/{}", 0),
        json!(patch_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::NOT_FOUND, res.status());

    let res = req(
        &client,
        Method::PATCH,
        &format!("http://{addr}/ui/worksheets/{}", worksheet.id),
        json!(patch_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/worksheets/{}", worksheet.id),
        json!(patch_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let worksheet_2 = res.json::<WorksheetResponse>().await.unwrap().data;
    assert_eq!(worksheet_2.name, patch_payload.name);
    assert_eq!(worksheet_2.content, patch_payload.content);

    let res = req(
        &client,
        Method::DELETE,
        &format!("http://{addr}/ui/worksheets/{}", worksheet.id),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    // Shouldn't exist
    let res = req(
        &client,
        Method::DELETE,
        &format!("http://{addr}/ui/worksheets/{}", worksheet.id),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::NOT_FOUND, res.status());
}
