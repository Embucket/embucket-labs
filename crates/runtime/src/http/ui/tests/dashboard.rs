#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::ui::dashboard::models::DashboardResponse;
use crate::http::ui::databases::models::DatabaseCreatePayload;
use crate::http::ui::queries::models::QueryCreatePayload;
use crate::http::ui::schemas::models::SchemaCreatePayload;
use crate::http::ui::tests::common::req;
use crate::http::ui::tests::common::{ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{Volume, VolumeCreatePayload, VolumeCreateResponse};
use crate::http::ui::worksheets::models::{WorksheetCreatePayload, WorksheetResponse};
use crate::tests::run_test_server;
use embucket_metastore::VolumeType as MetastoreVolumeType;
use embucket_metastore::{Database as MetastoreDatabase, Volume as MetastoreVolume};
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_dashboard() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ui/dashboard");
    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let dashboard: DashboardResponse = res.json().await.unwrap();
    assert_eq!(0, dashboard.data.total_databases);
    assert_eq!(0, dashboard.data.total_schemas);
    assert_eq!(0, dashboard.data.total_tables);
    assert_eq!(0, dashboard.data.total_queries);

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

    // Create database, Ok
    let expected1 = DatabaseCreatePayload {
        data: MetastoreDatabase {
            ident: "test1".to_string(),
            properties: None,
            volume: volume.data.name.clone(),
        }
        .into(),
    };
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
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected1.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected2.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected3.clone())).await;
    let _res = ui_test_op(addr, Op::Create, None, &Entity::Database(expected4.clone())).await;

    let res = req(&client, Method::GET, &url, String::new())
        .await
        .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let dashboard: DashboardResponse = res.json().await.unwrap();
    assert_eq!(4, dashboard.data.total_databases);
    assert_eq!(0, dashboard.data.total_schemas);
    assert_eq!(0, dashboard.data.total_tables);
    assert_eq!(0, dashboard.data.total_queries);

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
    let dashboard: DashboardResponse = res.json().await.unwrap();
    assert_eq!(4, dashboard.data.total_databases);
    assert_eq!(1, dashboard.data.total_schemas);
    assert_eq!(0, dashboard.data.total_tables);
    assert_eq!(0, dashboard.data.total_queries);

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
        worksheet_id: Some(worksheet.id),
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
    let dashboard: DashboardResponse = res.json().await.unwrap();
    assert_eq!(4, dashboard.data.total_databases);
    assert_eq!(1, dashboard.data.total_schemas);
    assert_eq!(1, dashboard.data.total_tables);
    assert_eq!(1, dashboard.data.total_queries);
}
