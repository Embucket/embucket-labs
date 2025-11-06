#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::databases::models::{DatabaseCreatePayload};
use crate::queries::models::QueryCreatePayload;
use crate::schemas::models::SchemaCreatePayload;
use crate::tables::models::{
    TableColumnsResponse, TablePreviewDataResponse, TableStatisticsResponse, TablesResponse,
};
use crate::tests::common::{Entity, Op, req, ui_test_op};
use crate::tests::server::run_test_server;
use crate::volumes::models::{VolumeCreatePayload, VolumeCreateResponse, VolumeType};
use crate::worksheets::{Worksheet, WorksheetCreatePayload, WorksheetResponse};
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_tables() {
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
    let WorksheetResponse(Worksheet {
        id: worksheet_id, ..
    }) = res.json().await.unwrap();

    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
        &format!("http://{addr}/ui/queries"),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
        &format!("http://{addr}/ui/queries"),
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
    let table: TableColumnsResponse = res.json().await.unwrap();
    assert_eq!(5, table.items.len());

    //table preview data
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1/rows",
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
            "http://{addr}/ui/databases/{}/schemas/{}/tables/tested1/rows?offset=1&limit=1",
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
    let TableStatisticsResponse(table) = res.json().await.unwrap();
    assert_eq!(0, table.total_bytes);
    assert_eq!(2, table.total_rows);

    //Create three more tables
    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
        &format!("http://{addr}/ui/queries"),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
        &format!("http://{addr}/ui/queries"),
        json!(query_payload).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
        &format!("http://{addr}/ui/queries"),
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
    assert_eq!("tested4".to_string(), tables.items.first().unwrap().name);

    //GET LIST Tables with cursor
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?offset=2",
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
    assert_eq!("tested2".to_string(), tables.items.first().unwrap().name);

    //Create a table with another name
    let query_payload = QueryCreatePayload {
        worksheet_id: Some(worksheet_id),
        async_exec: false,
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
        &format!("http://{addr}/ui/queries"),
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
            "tested"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(4, tables.items.len());
    assert_eq!("tested4".to_string(), tables.items.first().unwrap().name);

    //GET LIST Tables with search
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?search={}&orderDirection=ASC",
            database_name.clone(),
            schema_name.clone(),
            "tested"
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
            "tested"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(2, tables.items.len());
    assert_eq!("tested4".to_string(), tables.items.first().unwrap().name);

    //GET LIST Tables with cursor
    let res = req(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/databases/{}/schemas/{}/tables?search={}&offset=2",
            database_name.clone(),
            schema_name.clone(),
            "tested"
        ),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let tables: TablesResponse = res.json().await.unwrap();
    assert_eq!(2, tables.items.len());
    assert_eq!("tested2".to_string(), tables.items.first().unwrap().name);

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
