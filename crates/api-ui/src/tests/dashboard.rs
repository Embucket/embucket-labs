#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::dashboard::models::DashboardResponse;
use crate::databases::models::DatabaseCreatePayload;
use crate::queries::models::QueryCreatePayload;
use crate::schemas::models::SchemaCreatePayload;
use crate::tests::common::req;
use crate::tests::common::{Entity, Op, ui_test_op};
use crate::tests::server::run_test_server;
use crate::volumes::models::{VolumeCreatePayload, VolumeCreateResponse, VolumeType};
use crate::worksheets::models::{Worksheet, WorksheetCreatePayload, WorksheetResponse};
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_dashboard() {
    let addr = run_test_server();
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ui/dashboard");
    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let DashboardResponse(dashboard) = res.json().await.unwrap();
    assert_eq!(0, dashboard.total_databases);
    assert_eq!(0, dashboard.total_schemas);
    assert_eq!(0, dashboard.total_tables);
    assert_eq!(0, dashboard.total_queries);

    let res = ui_test_op(
        addr,
        Op::Create,
        None,
        &Entity::Volume(VolumeCreatePayload {
            name: "test_volume".to_string(),
            volume: VolumeType::Memory,
        }),
    )
    .await;
    let VolumeCreateResponse(volume) = res.json().await.unwrap();

    // Create database, Ok
    let expected1 = DatabaseCreatePayload {
        name: "test1".to_string(),
        volume_id: volume.id,
    };
    let expected2 = DatabaseCreatePayload {
        name: "test2".to_string(),
        volume_id: volume.id,
    };
    let expected3 = DatabaseCreatePayload {
        name: "test3".to_string(),
        volume_id: volume.id,
    };
    let expected4 = DatabaseCreatePayload {
        name: "test4".to_string(),
        volume_id: volume.id,
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
    let DashboardResponse(dashboard) = res.json().await.unwrap();
    assert_eq!(4, dashboard.total_databases);
    assert_eq!(0, dashboard.total_schemas);
    assert_eq!(0, dashboard.total_tables);
    assert_eq!(5, dashboard.total_queries);

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
            expected1.name.clone()
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
    let DashboardResponse(dashboard) = res.json().await.unwrap();
    assert_eq!(4, dashboard.total_databases);
    assert_eq!(1, dashboard.total_schemas);
    assert_eq!(0, dashboard.total_tables);
    //Since databases and schemas are created with sql
    assert_eq!(6, dashboard.total_queries);

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
    let WorksheetResponse(Worksheet {
        id: worksheet_id, ..
    }) = res.json().await.unwrap();

    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
            expected1.name.clone(),
            schema_name.clone(),
            "tested1"
        ),
        context: None,
    };

    let res = req(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let DashboardResponse(dashboard) = res.json().await.unwrap();
    assert_eq!(4, dashboard.total_databases);
    assert_eq!(1, dashboard.total_schemas);
    assert_eq!(1, dashboard.total_tables);
    //Since volumes, databases and schemas are created with sql
    assert_eq!(7, dashboard.total_queries);
}
