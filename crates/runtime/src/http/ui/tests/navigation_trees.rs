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

use crate::http::ui::databases::models::DatabaseCreatePayload;
use crate::http::ui::navigation_trees::models::NavigationTreesResponse;
use crate::http::ui::queries::models::QueryCreatePayload;
use crate::http::ui::schemas::models::SchemaCreatePayload;
use crate::http::ui::tests::common::req;
use crate::http::ui::tests::common::{ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{Volume, VolumeCreatePayload, VolumeCreateResponse};
use crate::http::ui::worksheets::models::{WorksheetCreatePayload, WorksheetResponse};
use crate::tests::run_icebucket_test_server;
use http::Method;
use icebucket_metastore::IceBucketVolumeType;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume};
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_databases_navigation() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ui/navigation-trees");
    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: NavigationTreesResponse = res.json().await.unwrap();
    assert_eq!(0, databases_navigation.items.len());

    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumeCreatePayload {
            data: Volume::from(IceBucketVolume {
                ident: String::new(),
                volume: IceBucketVolumeType::Memory,
            }),
        }),
    )
    .await;
    let volume = res.json::<VolumeCreateResponse>().await.unwrap();

    // Create database, Ok
    let expected1 = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "test1".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let expected2 = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "test2".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let expected3 = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "test3".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let expected4 = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "test4".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    //4 DBs
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected2.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected3.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected4.clone())).await;

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: NavigationTreesResponse = res.json().await.unwrap();
    assert_eq!(4, databases_navigation.items.len());

    let schema_name = "testing1".to_string();
    let payload = SchemaCreatePayload {
        name: schema_name.clone(),
    };
    //Create schema
    let res = req(
        &client,
        Method::POST,
        &format!(
            "http://{addr}/ui/databases/{}/schemas",
            expected1.data.name.clone()
        )
        .to_string(),
        json!(payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: NavigationTreesResponse = res.json().await.unwrap();
    assert_eq!(4, databases_navigation.items.len());
    assert_eq!(1, databases_navigation.items.first().unwrap().schemas.len());
    assert_eq!(0, databases_navigation.items.last().unwrap().schemas.len());

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets"),
        json!(WorksheetCreatePayload {
            name: "test".to_string(),
            content: String::new(),
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let worksheet = res.json::<WorksheetResponse>().await.unwrap().data;

    let query_payload = QueryCreatePayload {
        query: format!(
            "create or replace Iceberg TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    ETL_TSTAMP TEXT,
	    COLLECTOR_TSTAMP TEXT NOT NULL,
	    DVCE_CREATED_TSTAMP TEXT,
	    EVENT TEXT,
	    EVENT_ID TEXT);",
            expected1.data.name.clone(),
            schema_name.clone(),
            "tested1"
        ),
        context: None,
    };

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries?worksheet_id={}", worksheet.id),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: NavigationTreesResponse = res.json().await.unwrap();

    assert_eq!(
        1,
        databases_navigation
            .items
            .first()
            .unwrap()
            .schemas
            .first()
            .unwrap()
            .tables
            .len()
    );

    let res = req(
        &client,
        Method::GET,
        &format!("{url}?limit=2"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: NavigationTreesResponse = res.json().await.unwrap();
    assert_eq!(2, databases_navigation.items.len());
    assert_eq!("test1", databases_navigation.items.first().unwrap().name);
    let cursor = databases_navigation.next_cursor;
    let res = req(
        &client,
        Method::GET,
        &format!("{url}?cursor={cursor}"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_navigation: NavigationTreesResponse = res.json().await.unwrap();
    assert_eq!(2, databases_navigation.items.len());
    assert_eq!("test3", databases_navigation.items.first().unwrap().name);
}
