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

use crate::http::ui::databases::models::DatabasePayload;
use crate::http::ui::error::UIResponse;
use crate::http::ui::queries::models::QueryPayload;
use crate::http::ui::schemas::models::SchemaPayload;
use crate::http::ui::tables::models::TableResponse;
use crate::http::ui::tests::common::{req, ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::VolumePayload;
use crate::http::ui::worksheets::{WorksheetPayload, WorksheetResponse};
use crate::tests::run_icebucket_test_server;
use http::Method;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume};
use icebucket_metastore::{IceBucketSchema, IceBucketSchemaIdent, IceBucketVolumeType};
use serde_json::json;
use std::collections::HashMap;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_tables() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();

    // Create volume with empty name
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumePayload {
            data: IceBucketVolume {
                ident: String::new(),
                volume: IceBucketVolumeType::Memory,
            },
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
    let _res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Database(DatabasePayload {
            data: expected1.clone(),
        }),
    )
    .await;

    let schema_name = "testing1".to_string();

    let schema_expected1 = IceBucketSchema {
        ident: IceBucketSchemaIdent {
            schema: schema_name.clone(),
            database: database_name.clone(),
        },
        properties: Some(HashMap::new()),
    };

    let payload = SchemaPayload {
        data: schema_expected1.clone(),
    };

    //Create schema
    let res = req(
        &client,
        Method::POST,
        &format!(
            "http://{addr}/ui/databases/{}/schemas",
            database_name.clone()
        )
        .to_string(),
        json!(payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets"),
        json!(WorksheetPayload {
            name: Some("test".to_string()),
            content: None,
        })
        .to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let worksheet = res.json::<WorksheetResponse>().await.unwrap().data;

    let query_payload = QueryPayload::new(format!(
        "create or replace Iceberg TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT
	    );",
        database_name.clone(),
        schema_name.clone(),
        "tested1"
    ));

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let query_payload = QueryPayload::new(format!(
        "INSERT INTO {}.{}.{} (APP_ID, PLATFORM, EVENT)
        VALUES ('12345', 'iOS', 'login'),
               ('67890', 'Android', 'purchase')",
        database_name.clone(),
        schema_name.clone(),
        "tested1"
    ));

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets/{}/queries", worksheet.id),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let table: UIResponse<TableResponse> = res.json().await.unwrap();
    assert_eq!(3, table.data.columns.len());
    assert_eq!(2, table.data.total_rows);
}
