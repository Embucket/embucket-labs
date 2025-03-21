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
use crate::http::ui::models::databases_navigation::NavigationDatabase;
use crate::tests::run_icebucket_test_server;
use http::Method;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume};
use icebucket_metastore::{IceBucketSchema, IceBucketSchemaIdent, IceBucketVolumeType};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_databases_navigation() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ui/databases-navigation");
    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: Vec<NavigationDatabase> = res.json().await.unwrap();
    assert_eq!(0, databases_navigation.len());

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
    let volume = res.json::<IceBucketVolume>().await.unwrap();

    // Create database, Ok
    let expected1 = IceBucketDatabase {
        ident: "test1".to_string(),
        properties: None,
        volume: volume.ident.clone(),
    };
    let expected2 = IceBucketDatabase {
        ident: "test2".to_string(),
        properties: None,
        volume: volume.ident.clone(),
    };
    //2 DBs
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected2.clone())).await;

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: Vec<NavigationDatabase> = res.json().await.unwrap();
    assert_eq!(2, databases_navigation.len());

    // Create schema, Ok
    let expected1 = IceBucketSchema {
        ident: IceBucketSchemaIdent {
            schema: "testing1".to_string(),
            database: expected1.ident.clone(),
        },
        properties: None,
    };
    //1 SCHEMA
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Schema(expected1.clone())).await;

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: Vec<NavigationDatabase> = res.json().await.unwrap();
    assert_eq!(2, databases_navigation.len());
    assert_eq!(1, databases_navigation.first().unwrap().schemas.len());
    assert_eq!(0, databases_navigation.last().unwrap().schemas.len());
}
