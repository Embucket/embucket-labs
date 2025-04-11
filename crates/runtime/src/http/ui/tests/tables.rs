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
use crate::http::ui::queries::models::QueryCreatePayload;
use crate::http::ui::schemas::models::SchemaCreatePayload;
use crate::http::ui::tables::models::{
    TableColumnsInfoResponse, TablePreviewDataResponse, TableStatisticsResponse, TablesResponse,
};
use crate::http::ui::tests::common::{req, ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{VolumeCreatePayload, VolumeCreateResponse};
use crate::http::ui::worksheets::{WorksheetCreatePayload, WorksheetResponse};
use crate::tests::run_icebucket_test_server;
use http::Method;
use icebucket_metastore::IceBucketVolumeType;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume};
use serde_json::json;

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
        &Entity::Volume(VolumeCreatePayload {
            data: IceBucketVolume {
                ident: String::new(),
                volume: IceBucketVolumeType::Memory,
            }
            .into(),
        }),
    )
    .await;
    let volume: VolumeCreateResponse = res.json().await.unwrap();

    let database_name = "test1".to_string();
    // Create database, Ok
    let expected1 = IceBucketDatabase {
        ident: database_name.clone(),
        properties: None,
        volume: volume.data.name.clone(),
    };
    let _res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Database(DatabaseCreatePayload {
            data: expected1.clone().into(),
        }),
    )
    .await;

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
            "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
            database_name.clone(),
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

    let query_payload = QueryCreatePayload {
        query: format!(
            "INSERT INTO {}.{}.{} (APP_ID, PLATFORM, EVENT, TXN_ID, EVENT_TIME)
        VALUES ('12345', 'iOS', 'login', '123456', '2021-01-01T00:00:00'),
               ('67890', 'Android', 'purchase', '456789', '2021-01-01T00:02:00')",
            database_name.clone(),
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

    //table columns info
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1/columns",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let table: TableColumnsInfoResponse = res.json().await.unwrap();
    assert_eq!(5, table.items.len());

    //table preview data
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1/preview",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let table: TablePreviewDataResponse = res.json().await.unwrap();
    assert_eq!(5, table.items.len());
    assert_eq!(2, table.items.first().unwrap().rows.len());
    assert_eq!(
        "2021-01-01T00:00:00",
        table.items.last().unwrap().rows.first().unwrap().data
    );

    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1/preview?offset=1&limit=1",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let table: TablePreviewDataResponse = res.json().await.unwrap();
    assert_eq!(5, table.items.len());
    assert_eq!(1, table.items.first().unwrap().rows.len());
    assert_eq!(
        "2021-01-01T00:02:00",
        table.items.last().unwrap().rows.first().unwrap().data
    );

    //table info
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1/statistics",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let table: TableStatisticsResponse = res.json().await.unwrap();
    assert_eq!(0, table.data.total_bytes);
    assert_eq!(0, table.data.total_rows);

    //Create three more tables
    let query_payload = QueryCreatePayload {
        query: format!(
            "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
            database_name.clone(),
            schema_name.clone(),
            "tested2"
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

    let query_payload = QueryCreatePayload {
        query: format!(
            "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
            database_name.clone(),
            schema_name.clone(),
            "tested3"
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

    let query_payload = QueryCreatePayload {
        query: format!(
            "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
            database_name.clone(),
            schema_name.clone(),
            "tested4"
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

    //GET LIST Tables
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(4, tables.items.len());
    assert_eq!("TABLE".to_string(), tables.items.first().unwrap().r#type);
    assert_eq!(0, tables.items.first().unwrap().total_bytes);
    assert_eq!(0, tables.items.first().unwrap().total_rows);

    //GET LIST Tables with Limit
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?limit=2",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(2, tables.items.len());
    assert_eq!("tested1".to_string(), tables.items.first().unwrap().name);
    let cursor = tables.next_cursor;

    //GET LIST Tables with cursor
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?cursor={cursor}",
            database_name.clone(),
            schema_name.clone()
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(2, tables.items.len());
    assert_eq!("tested3".to_string(), tables.items.first().unwrap().name);

    //Create a table with another name
    let query_payload = QueryCreatePayload {
        query: format!(
            "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
            database_name.clone(),
            schema_name.clone(),
            "named1"
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

    //GET LIST Tables with search
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?search={}",
            database_name.clone(),
            schema_name.clone(),
            "tes"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(4, tables.items.len());
    assert_eq!("tested1".to_string(), tables.items.first().unwrap().name);

    //GET LIST Tables with search and limit
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?search={}&limit=2",
            database_name.clone(),
            schema_name.clone(),
            "tes"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(2, tables.items.len());
    assert_eq!("tested1".to_string(), tables.items.first().unwrap().name);
    let cursor = tables.next_cursor;

    //GET LIST Tables with cursor
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?search={}&cursor={cursor}",
            database_name.clone(),
            schema_name.clone(),
            "tes"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(2, tables.items.len());
    assert_eq!("tested3".to_string(), tables.items.first().unwrap().name);

    //GET LIST Tables with search for the other table
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?search={}",
            database_name.clone(),
            schema_name.clone(),
            "nam"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(1, tables.items.len());
    assert_eq!("named1".to_string(), tables.items.first().unwrap().name);
}
