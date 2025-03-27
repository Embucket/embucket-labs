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
use crate::http::ui::databases::models::{
    DatabaseCreatePayload, DatabaseResponse, DatabasesResponse,
};
use crate::http::ui::tests::common::{ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{Volume, VolumeCreatePayload, VolumeCreateResponse};
use crate::tests::run_icebucket_test_server;
use icebucket_metastore::IceBucketVolumeType;
use icebucket_metastore::{IceBucketDatabase, IceBucketVolume};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_databases_metastore_update_bug() {
    let addr = run_icebucket_test_server().await;

    // Create volume with empty name
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumeCreatePayload {
            data: Volume::from(IceBucketVolume {
                ident: String::from("t"),
                volume: IceBucketVolumeType::Memory,
            }),
        }),
    )
    .await;
    let volume = res.json::<VolumeCreateResponse>().await.unwrap();

    // Create database, Ok
    let expected = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "test".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let created_database = res.json::<DatabaseResponse>().await.unwrap();
    assert_eq!(expected.data, created_database.data);

    // Update database test -> new-test, Ok
    let new_database = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "new-test".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(
        addr,
        Op::Update,
        Some(&Entity::Database(DatabaseCreatePayload {
            data: created_database.data.clone(),
        })),
        &Entity::Database(new_database.clone()),
    )
    .await;
    assert_eq!(http::StatusCode::OK, res.status());
    let renamed_database = res.json::<DatabaseResponse>().await.unwrap();
    assert_eq!(new_database.data, renamed_database.data); // server confirmed it's renamed

    // Bug discovered: Database not updated as old name is still accessable

    // get non existing database using old name, expected error 404
    let res = ui_test_op(
        addr,
        Op::Get,
        None,
        &Entity::Database(DatabaseCreatePayload {
            data: created_database.data.clone(),
        }),
    )
    .await;
    assert_eq!(http::StatusCode::NOT_FOUND, res.status());
    let error = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(http::StatusCode::NOT_FOUND, error.status_code);

    // Get existing database using new name, expected Ok
    let res = ui_test_op(
        addr,
        Op::Get,
        None,
        &Entity::Database(DatabaseCreatePayload {
            data: renamed_database.data.clone(),
        }),
    )
    .await;
    assert_eq!(http::StatusCode::OK, res.status());
    let error = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(http::StatusCode::OK, error.status_code);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_databases() {
    let addr = run_icebucket_test_server().await;

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
    let volume = res.json::<VolumeCreateResponse>().await.unwrap();

    // Create database with empty name, error 400
    let expected = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: String::new(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected.clone())).await;
    assert_eq!(http::StatusCode::BAD_REQUEST, res.status());
    let error = res.json::<ErrorResponse>().await.unwrap();
    assert_eq!(http::StatusCode::BAD_REQUEST, error.status_code);

    let stub = Entity::Database(expected);

    // List databases count = 0, Ok
    let res = ui_test_op(addr, Op::List, None, &stub.clone()).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let databases = res.json::<DatabasesResponse>().await.unwrap();
    assert_eq!(0, databases.items.len());

    // Create database, Ok
    let expected = DatabaseCreatePayload {
        data: IceBucketDatabase {
            ident: "test".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let created_database = res.json::<DatabaseResponse>().await.unwrap();
    assert_eq!(expected.data, created_database.data);

    // List databases, Ok
    let res = ui_test_op(addr, Op::List, None, &stub.clone()).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let databases = res.json::<DatabasesResponse>().await.unwrap();
    assert_eq!(1, databases.items.len());

    // Delete database, Ok
    let res = ui_test_op(
        addr,
        Op::Delete,
        Some(&Entity::Database(DatabaseCreatePayload {
            data: created_database.data.clone(),
        })),
        &stub,
    )
    .await;
    assert_eq!(http::StatusCode::OK, res.status());

    // List databases, Ok
    let res = ui_test_op(addr, Op::List, None, &stub.clone()).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let databases = res.json::<DatabasesResponse>().await.unwrap();
    assert_eq!(0, databases.items.len());
}
