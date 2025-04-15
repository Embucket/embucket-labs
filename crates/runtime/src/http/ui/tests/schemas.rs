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

use crate::http::ui::databases::models::{Database, DatabaseCreatePayload};
use crate::http::ui::schemas::models::{SchemaCreatePayload, SchemasResponse};
use crate::http::ui::tests::common::{req, ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{Volume, VolumeCreatePayload, VolumeCreateResponse};
use crate::tests::run_icebucket_test_server;
use http::Method;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume, IceBucketVolumeType};
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_schemas() {
    let addr = run_icebucket_test_server().await;
    let client = reqwest::Client::new();

    // Create volume with empty name
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
            data: Database::from(expected1.clone()),
        }),
    )
    .await;

    let schema_name = "testing1".to_string();
    let payload1 = SchemaCreatePayload {
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
        json!(payload1).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let schema_name = "testing2".to_string();
    let payload2 = SchemaCreatePayload {
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
        json!(payload2).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let schema_name = "testing3".to_string();
    let payload3 = SchemaCreatePayload {
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
        json!(payload3).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    //Get list schemas
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas",
            database_name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(3, schemas_response.items.len());

    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?limit=1",
            database_name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(1, schemas_response.items.len());
    assert_eq!(
        "testing1".to_string(),
        schemas_response.items.first().unwrap().name
    );
    let cursor = schemas_response.next_cursor;
    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?cursor={cursor}",
            database_name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(2, schemas_response.items.len());
    assert_eq!(
        "testing2".to_string(),
        schemas_response.items.first().unwrap().name
    );

    let schema_name = "naming1".to_string();
    let payload_another = SchemaCreatePayload {
        name: schema_name.clone(),
    };
    //Create schema with another name
    let res = req(
        &client,
        Method::POST,
        &format!(
            "http://{addr}/ui/databases/{}/schemas",
            database_name.clone()
        )
        .to_string(),
        json!(payload_another).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    //Get list schemas with search
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}",
            database_name.clone(),
            "tes"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(3, schemas_response.items.len());

    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}&limit=1",
            database_name.clone(),
            "tes"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(1, schemas_response.items.len());
    assert_eq!(
        "testing1".to_string(),
        schemas_response.items.first().unwrap().name
    );
    let cursor = schemas_response.next_cursor;
    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}&cursor={cursor}",
            database_name.clone(),
            "tes"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(2, schemas_response.items.len());
    assert_eq!(
        "testing2".to_string(),
        schemas_response.items.first().unwrap().name
    );

    //Get list schemas with search for another name
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}",
            database_name.clone(),
            "nam"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(1, schemas_response.items.len());
    assert_eq!("naming1", schemas_response.items.first().unwrap().name);

    //Delete existing schema
    let res = req(
        &client,
        Method::DELETE,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}",
            database_name.clone(),
            payload1.name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    //Delete existing schema
    let res = req(
        &client,
        Method::DELETE,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}",
            database_name.clone(),
            payload2.name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    //Delete existing schema
    let res = req(
        &client,
        Method::DELETE,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}",
            database_name.clone(),
            payload3.name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    //Delete existing schema
    let res = req(
        &client,
        Method::DELETE,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}",
            database_name.clone(),
            payload_another.name.clone()
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
}
