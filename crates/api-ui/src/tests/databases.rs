#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::databases::models::{DatabaseCreatePayload, DatabaseResponse, DatabasesResponse};
use crate::error::ErrorResponse;
use crate::tests::common::{Entity, Op, req, ui_test_op};
use crate::tests::server::run_test_server;
use crate::volumes::models::{Volume, VolumeCreatePayload, VolumeCreateResponse};
use core_metastore::VolumeType as MetastoreVolumeType;
use core_metastore::{Database as MetastoreDatabase, Volume as MetastoreVolume};
use http::Method;

#[tokio::test]
#[allow(clippy::too_many_lines)]
#[should_panic(
    expected = "Failed to get error response: reqwest::Error { kind: Decode, source: Error(\"missing field `message`\", line: 1, column: 32) }"
)]
async fn test_ui_databases_metastore_update_bug() {
    let addr = run_test_server().await;

    // Create volume with empty name
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumeCreatePayload {
            data: Volume::from(MetastoreVolume {
                ident: String::from("t"),
                volume: MetastoreVolumeType::Memory,
            }),
        }),
    )
    .await;
    let volume = res
        .json::<VolumeCreateResponse>()
        .await
        .expect("Failed to create volume");

    // Create database, Ok
    let expected = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "test".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let created_database = res
        .json::<DatabaseResponse>()
        .await
        .expect("Failed to create database");
    assert_eq!(expected.data, created_database.data);

    // Update database test -> new-test, Ok
    let new_database = DatabaseCreatePayload {
        data: MetastoreDatabase {
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
    let renamed_database = res
        .json::<DatabaseResponse>()
        .await
        .expect("Failed to update database");
    assert_eq!(new_database.data, renamed_database.data); // server confirmed it's renamed

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
    // TODO: Fix this test case, it should return 404
    // Database not updated as old name is still accessable
    let error = res
        .json::<ErrorResponse>()
        .await
        .expect("Failed to get error response");
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
    let error = res
        .json::<ErrorResponse>()
        .await
        .expect("Failed to get error response");
    assert_eq!(http::StatusCode::OK, error.status_code);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_databases() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    // Create volume with empty name
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumeCreatePayload {
            data: Volume::from(MetastoreVolume {
                ident: String::new(),
                volume: MetastoreVolumeType::Memory,
            }),
        }),
    )
    .await;
    let volume = res.json::<VolumeCreateResponse>().await.unwrap();

    // Create database with empty name, error 400
    let expected = DatabaseCreatePayload {
        data: MetastoreDatabase {
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
    let expected1 = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "test".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let created_database = res.json::<DatabaseResponse>().await.unwrap();
    assert_eq!(expected1.data, created_database.data);

    let expected2 = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "test2".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let expected3 = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "test3".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let expected4 = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "test4".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    //4 DBs
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected2.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected3.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected4.clone())).await;

    // List databases, Ok
    let res = ui_test_op(addr, Op::List, None, &stub.clone()).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let databases = res.json::<DatabasesResponse>().await.unwrap();
    assert_eq!(4, databases.items.len());

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
    assert_eq!(3, databases.items.len());

    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());

    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?limit=2",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(2, databases_response.items.len());
    assert_eq!(
        "test".to_string(),
        databases_response.items.first().unwrap().name
    );
    let cursor = databases_response.next_cursor;
    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?cursor={cursor}",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(2, databases_response.items.len());
    assert_eq!(
        "test3".to_string(),
        databases_response.items.first().unwrap().name
    );

    // Create database with another name, Ok
    let expected_another = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "name".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Database(expected_another.clone()),
    )
    .await;
    assert_eq!(http::StatusCode::OK, res.status());

    //Get list schemas with search
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?search={}", "tes").to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(4, databases_response.items.len());
    assert_eq!(
        "test".to_string(),
        databases_response.items.first().unwrap().name
    );

    //Get list schemas with search
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?search={}&limit=2", "tes").to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(2, databases_response.items.len());
    assert_eq!(
        "test".to_string(),
        databases_response.items.first().unwrap().name
    );
    let cursor = databases_response.next_cursor;

    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases?search={}&cursor={cursor}",
            "tes"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(2, databases_response.items.len());
    assert_eq!(
        "test3".to_string(),
        databases_response.items.first().unwrap().name
    );

    //Get list schemas with search fro another name
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?search={}", "nam").to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(1, databases_response.items.len());
    assert_eq!(
        "name".to_string(),
        databases_response.items.first().unwrap().name
    );
}
