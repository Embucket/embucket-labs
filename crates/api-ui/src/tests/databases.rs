#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::databases::models::{
    DatabaseCreatePayload, DatabaseCreateResponse, DatabaseUpdateResponse, DatabasesResponse,
};
use crate::error::ErrorResponse;
use crate::tests::common::{Entity, Op, req, ui_test_op};
use crate::tests::server::run_test_server;
use crate::volumes::models::{VolumeCreatePayload, VolumeCreateResponse, VolumeType};
use http::Method;

#[tokio::test]
#[allow(clippy::too_many_lines)]
#[should_panic(
    expected = "Failed to get error response: reqwest::Error { kind: Decode, source: Error(\"missing field `message`\", line: 1, column: 120) }"
)]
async fn test_ui_databases_metastore_update_bug() {
    let addr = run_test_server().await;

    // Create volume with empty name
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumeCreatePayload {
            name: String::from("t"),
            volume: VolumeType::Memory,
        }),
    )
    .await;
    let VolumeCreateResponse(volume) = res.json().await.unwrap();

    // Create database, Ok
    let expected = DatabaseCreatePayload {
        name: "test".to_string(),
        volume: volume.name.clone(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let DatabaseCreateResponse(created_database) = res.json().await.unwrap();
    assert_eq!(expected.name, created_database.name);
    assert_eq!(expected.volume, created_database.volume);

    // Update database test -> new-test, Ok
    let new_database = DatabaseCreatePayload {
        name: "new-test".to_string(),
        volume: volume.name.clone(),
    };
    let res = ui_test_op(
        addr,
        Op::Update,
        Some(&Entity::Database(DatabaseCreatePayload {
            name: created_database.name.clone(),
            volume: created_database.volume.clone(),
        })),
        &Entity::Database(new_database.clone()),
    )
    .await;
    assert_eq!(http::StatusCode::OK, res.status());
    let DatabaseUpdateResponse(renamed_database) = res.json().await.unwrap();
    assert_eq!(new_database.name, renamed_database.name); // server confirmed it's renamed
    assert_eq!(new_database.volume, renamed_database.volume);

    // get non existing database using old name, expected error 404
    let res = ui_test_op(
        addr,
        Op::Get,
        None,
        &Entity::Database(DatabaseCreatePayload {
            name: created_database.name.clone(),
            volume: created_database.volume.clone(),
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
            name: renamed_database.name.clone(),
            volume: renamed_database.volume.clone(),
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
            name: String::new(),
            volume: VolumeType::Memory,
        }),
    )
    .await;
    let VolumeCreateResponse(volume) = res.json().await.unwrap();

    // Create database with empty name, error 400
    let expected = DatabaseCreatePayload {
        name: String::new(),
        volume: volume.name.clone(),
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
        name: "test".to_string(),
        volume: volume.name.clone(),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;
    assert_eq!(http::StatusCode::OK, res.status());
    let DatabaseCreateResponse(created_database) = res.json().await.unwrap();
    assert_eq!(expected1.name, created_database.name);
    assert_eq!(expected1.volume, created_database.volume);

    let expected2 = DatabaseCreatePayload {
        name: "test2".to_string(),
        volume: volume.name.clone(),
    };
    let expected3 = DatabaseCreatePayload {
        name: "test3".to_string(),
        volume: volume.name.clone(),
    };
    let expected4 = DatabaseCreatePayload {
        name: "test4".to_string(),
        volume: volume.name.clone(),
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
            name: created_database.name.clone(),
            volume: created_database.volume.clone(),
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

    //Get list databases with parameters
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
    //Get list databases with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?offset=2",).to_string(),
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
        name: "name".to_string(),
        volume: volume.name.clone(),
    };
    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Database(expected_another.clone()),
    )
    .await;
    assert_eq!(http::StatusCode::OK, res.status());

    //Get list databases with search
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

    //Get list databases with search
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases?search={}&orderDirection=ASC",
            "tes"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let databases_response: DatabasesResponse = res.json().await.unwrap();
    assert_eq!(4, databases_response.items.len());
    //Since we are using ASC, the first element is the one with the name "test2",
    // because we deleted "test" and added it back, and we are sorting by 'created_at'
    assert_eq!(
        "test2".to_string(),
        databases_response.items.first().unwrap().name
    );
    assert_eq!(
        "test".to_string(),
        databases_response.items.last().unwrap().name
    );

    //Get list databases with search
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

    //Get list databases with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/databases?search={}&offset=2", "test").to_string(),
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

    //Get list databases with search fro another name
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
