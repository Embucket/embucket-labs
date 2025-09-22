#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::error::ErrorResponse;
use crate::queries::models::{
    Column, QueriesResponse, QueryCreatePayload, QueryGetResponse, QueryRecord, QueryStatus,
    ResultSet,
};
use crate::tests::common::http_req;
use crate::tests::server::run_test_server;
use crate::worksheets::models::{Worksheet, WorksheetCreatePayload, WorksheetsResponse};
use http::Method;
use serde_json::json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_queries_no_worksheet() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let _ = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(0),
            query: "SELECT 1".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");

    let history_resp = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(history_resp.items.len(), 1);
    let query_record_id = history_resp.items[0].id;

    let QueryGetResponse(query_record) = http_req::<QueryGetResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries/{query_record_id}"),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(query_record.id, query_record_id);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_queries_with_worksheet() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let worksheet = http_req::<Worksheet>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/worksheets"),
        json!(WorksheetCreatePayload {
            name: String::new(),
            content: String::new(),
        })
        .to_string(),
    )
    .await
    .expect("Error creating worksheet");
    assert!(worksheet.id > 0);

    let res = http_req::<()>(
        &client,
        Method::DELETE,
        &format!("http://{addr}/ui/queries?worksheetId={}", worksheet.id),
        String::new(),
    )
    .await
    .expect_err("Error expected METHOD_NOT_ALLOWED");
    assert_eq!(http::StatusCode::METHOD_NOT_ALLOWED, res.status);

    // Bad payload
    let res = http_req::<()>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        String::new(),
    )
    .await
    .expect_err("Error expected: BAD_REQUEST");
    assert_eq!(http::StatusCode::BAD_REQUEST, res.status);

    let query = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT 1, 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");
    assert_eq!(
        query.result,
        ResultSet {
            columns: vec![
                Column {
                    name: "Int64(1)".to_string(),
                    r#type: "fixed".to_string(),
                },
                Column {
                    name: "Int64(2)".to_string(),
                    r#type: "fixed".to_string(),
                }
            ],
            rows: serde_json::from_str("[[1,2]]").unwrap()
        }
    );

    let query2 = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");

    assert_eq!(
        query2.result,
        ResultSet {
            columns: vec![Column {
                name: "Int64(2)".to_string(),
                r#type: "fixed".to_string(),
            }],
            rows: serde_json::from_str("[[2]]").unwrap()
        }
    );
    // assert_eq!(query_run_resp2.result, "[{\"Int64(2)\":2}]");

    let res = http_req::<()>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT foo".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect_err("Error expected: UNPROCESSABLE_ENTITY");
    assert_eq!(http::StatusCode::UNPROCESSABLE_ENTITY, res.status);
    let error_response =
        serde_json::from_str::<ErrorResponse>(&res.body).expect("Failed to parse ErrorResponse");
    assert_eq!(
        error_response.status_code,
        http::StatusCode::UNPROCESSABLE_ENTITY
    );

    // second fail
    let res = http_req::<()>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            query: "SELECT foo".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect_err("Error expected: UNPROCESSABLE_ENTITY");
    assert_eq!(http::StatusCode::UNPROCESSABLE_ENTITY, res.status);
    let error_response =
        serde_json::from_str::<ErrorResponse>(&res.body).expect("Failed to parse ErrorResponse");
    assert_eq!(
        error_response.status_code,
        http::StatusCode::UNPROCESSABLE_ENTITY
    );

    // get all=4
    let queries = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries?worksheetId={}", worksheet.id),
        String::new(),
    )
    .await
    .expect("Error getting queries")
    .items;
    assert_eq!(queries.len(), 4);

    // get 2
    let queries_response = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/queries?worksheetId={}&limit=2",
            worksheet.id
        ),
        String::new(),
    )
    .await
    .expect("Failed to get queries");
    let queries = queries_response.items;
    assert_eq!(queries.len(), 2);

    // check items returned in descending order
    assert_eq!(queries[0].status, QueryStatus::Failed);
    assert_eq!(queries[1].status, QueryStatus::Failed);

    // get rest
    let queries2 = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries?worksheetId={}", worksheet.id),
        String::new(),
    )
    .await
    .expect("Failed to get queries")
    .items;

    assert_eq!(queries2.len(), 2);
    // check items returned in descending order
    assert_eq!(queries2[0].status, QueryStatus::Successful);
    assert_eq!(queries2[0].result, query2.result);
    assert_eq!(queries2[1].status, QueryStatus::Successful);
    assert_eq!(queries2[1].result, query.result);

    // get worksheet with queries
    // tesing regression: "Deserialize error: missing field `id` at line 1 column 2"
    let worksheets = http_req::<WorksheetsResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/worksheets"),
        String::new(),
    )
    .await
    .expect("Failed to get worksheets");
    assert_eq!(worksheets.items.len(), 1);
}
