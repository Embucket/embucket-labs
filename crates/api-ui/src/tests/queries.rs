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
            async_exec: false,
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
    assert_eq!(history_resp.items.len(), 2);
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
            async_exec: false,
            query: "SELECT 1, 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");

    let result_set = http_req::<ResultSet>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries/{}/result", query.id),
        String::new(),
    )
    .await
    .expect("Get query result error");

    assert_eq!(
        result_set,
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
            async_exec: false,
            query: "SELECT 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");

    let result_set = http_req::<ResultSet>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries/{}/result", query2.id),
        String::new(),
    )
    .await
    .expect("Get query result error");

    assert_eq!(
        result_set,
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
            async_exec: false,
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
            async_exec: false,
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

    assert_eq!(queries[0].status, QueryStatus::Failed);
    assert_eq!(queries[1].status, QueryStatus::Failed);

    // get rest
    let queries2 = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/queries?worksheetId={}&offset=2",
            worksheet.id
        ),
        String::new(),
    )
    .await
    .expect("Failed to get queries")
    .items;

    assert_eq!(queries2.len(), 2);
    assert_eq!(queries2[0].status, QueryStatus::Successful);
    assert_eq!(queries2[1].status, QueryStatus::Successful);

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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_queries_search() {
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

    let _ = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            async_exec: false,
            query: "SELECT 1, 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");

    let _ = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            async_exec: false,
            query: "SELECT 2".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect("Create query error");

    let _ = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: Some(worksheet.id),
            async_exec: false,
            query: "CREATE".to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .expect_err("Create query error");

    // get search by query
    let queries = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!(
            "http://{addr}/ui/queries?worksheetId={}&search=SELECT&orderDirection=ASC",
            worksheet.id
        ),
        String::new(),
    )
    .await
    .expect("Error getting queries")
    .items;
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0].query, "SELECT 1, 2");
    assert_eq!(queries[1].query, "SELECT 2");

    // get search by status
    let queries = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries?search=failed",),
        String::new(),
    )
    .await
    .expect("Failed to get queries")
    .items;
    assert_eq!(queries.len(), 2);
    // since the search itself uses the word `failed` to look for the status, we also get the select search query
    assert_eq!(queries[0].status, QueryStatus::Running);
    assert_eq!(queries[1].status, QueryStatus::Failed);

    // get with parameters
    let queries = http_req::<QueriesResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries?worksheetId={}&search=SELECT&orderBy=start_time&OrderDirection=DESC", worksheet.id),
        String::new(),
    )
        .await
        .expect("Failed to get queries")
        .items;
    // check items returned in descending order
    assert_eq!(queries.len(), 2);
    assert!(queries[0].start_time > queries[1].start_time);
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn test_ui_async_query_infer_default_exec_mode() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    // asyncExec = true by default
    let payload = r#"{"query":"select 1","worksheetId": null,"context":{"database":"embucket","schema":"embucket"}}"#;

    // let payload = json!(QueryCreatePayload {
    //     worksheet_id: Some(0),
    //     async_exec: true,
    //     query: "SELECT 1".to_string(),
    //     context: None,
    // })
    // .to_string();

    // submit query asynchronously async_exec=true by default
    let query_record = http_req::<QueryRecord>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/queries"),
        payload.to_string(),
    )
    .await
    .expect("Create query error");

    assert_eq!(query_record.status, QueryStatus::Running);

    http_req::<()>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries/{}/result", query_record.id),
        String::new(),
    )
    .await
    .expect_err("Get query error");

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let QueryGetResponse(query_record) = http_req::<QueryGetResponse>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries/{}", query_record.id),
        String::new(),
    )
    .await
    .expect("Get query error");

    assert_eq!(query_record.status, QueryStatus::Successful);

    let expected_result =
        ResultSet::try_from(r#"{"columns":[{"name":"Int64(1)","type":"fixed"}],"rows":[[1]]}"#)
            .expect("Failed to deserialize json snippet #2");

    let result_set = http_req::<ResultSet>(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/queries/{}/result", query_record.id),
        String::new(),
    )
    .await
    .expect("Get query error");

    assert_eq!(expected_result, result_set);
}
