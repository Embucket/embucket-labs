#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::databases::models::DatabaseCreatePayload;
use crate::schemas::models::{SchemaCreatePayload, SchemasResponse};
use crate::tests::common::{Entity, Op, req, ui_test_op};
use crate::tests::server::run_test_server;
use crate::volumes::models::{VolumeCreatePayload, VolumeCreateResponse, VolumeType};
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_schemas() {
    let addr = run_test_server();
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

    let database_name = "test1".to_string();
    // Create database, Ok

    let _res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Database(DatabaseCreatePayload {
            name: database_name.clone(),
            volume: volume.name.clone(),
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
        "testing3".to_string(),
        schemas_response.items.first().unwrap().name
    );
    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?offset=1",
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
            "testing"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(3, schemas_response.items.len());

    //Get list schemas with search
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}&orderDirection=ASC",
            database_name.clone(),
            "testing"
        )
        .to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let schemas_response: SchemasResponse = res.json().await.unwrap();
    assert_eq!(
        vec![
            "testing1".to_string(),
            "testing2".to_string(),
            "testing3".to_string()
        ],
        schemas_response
            .items
            .into_iter()
            .map(|s| s.name)
            .collect::<Vec<String>>()
    );

    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}&limit=1",
            database_name.clone(),
            "testing"
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
        "testing3".to_string(),
        schemas_response.items.first().unwrap().name
    );
    //Get list schemas with parameters
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas?search={}&offset=1",
            database_name.clone(),
            "testing"
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
