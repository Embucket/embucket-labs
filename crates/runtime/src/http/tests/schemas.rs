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

use crate::http::tests::common::req;
use crate::http::tests::common::{ui_test_op, Entity, Op};
use crate::http::ui::models::schemas::CreateSchemaPayload;
use crate::tests::run_icebucket_test_server;
use http::Method;
use serde_json::json;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume, IceBucketSchema, IceBucketVolumeType};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_databases_navigation() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();

    // Create volume with empty name
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(IceBucketVolume {
            ident: String::new(),
            volume: IceBucketVolumeType::Memory,
        }),
    )
        .await;
    let volume: IceBucketVolume = res.json().await.unwrap();

    let database_name = "test1".to_string();
    // Create database, Ok
    let expected1 = IceBucketDatabase {
        ident: database_name.clone(),
        properties: None,
        volume: volume.ident.clone(),
    };
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;

    let payload = CreateSchemaPayload {
        name: "testing1".to_string(),
    };

    //Delete non existing schema
    let res = req(
        &client,
        Method::DELETE,
        &format!("http://addr/ui/databases/{}/schemas/{}", database_name.clone(), payload.name.clone()).to_string(),
        String::new(),
    )
        .await
        .unwrap();
    assert_eq!(http::StatusCode::BAD_REQUEST, res.status());

    //Create schema
    let res = req(
        &client,
        Method::POST,
        &format!("http://addr/ui/databases/{}/schemas/", database_name.clone()).to_string(),
        json!(payload).to_string(),
    )
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let _schema: IceBucketSchema = res.json().await.unwrap();

    //Delete existing schema
    let res = req(
        &client,
        Method::DELETE,
        &format!("http://addr/ui/databases/{}/schemas/{}", database_name.clone(), payload.name.clone()).to_string(),
        String::new(),
    )
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
}

