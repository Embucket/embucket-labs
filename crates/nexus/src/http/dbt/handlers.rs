use crate::error::AppError;
use crate::http::dbt::schemas::{
    JsonResponse, LoginRequestBody, LoginRequestQuery, QueryRequest, QueryRequestBody, ResponseData,
};
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use flate2::read::GzDecoder;
use regex::Regex;
use serde_json::json;
use std::io::Read;
use std::result::Result;
use uuid::Uuid;

pub async fn login(
    State(state): State<AppState>,
    Query(query): Query<LoginRequestQuery>,
    body: Bytes,
) -> Result<Json<(serde_json::value::Value)>, AppError> {
    // Decompress the gzip-encoded body
    let mut d = GzDecoder::new(&body[..]);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();

    // Deserialize the JSON body
    let body_json: LoginRequestBody = serde_json::from_str(&s).unwrap();

    println!("Received login request: {:?}", query);
    println!("Body data parameters: {:?}", body_json);
    let token = uuid::Uuid::new_v4().to_string();

    // Save warehouse id and db name in state
    for warehouse in state
        .control_svc
        .list_warehouses()
        .await?
        .into_iter()
        .filter(|w| w.name == query.warehouse)
    {
        state.dbt_sessions.lock().await.insert(
            token.clone(),
            format!("{}.{}", warehouse.id, query.database_name),
        );
    }
    Ok(Json(json!({"data": {"token": token}, "success": true})))
}

pub async fn query(
    State(state): State<AppState>,
    Query(query): Query<QueryRequest>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<JsonResponse>, AppError> {
    // Decompress the gzip-encoded body
    let mut d = GzDecoder::new(&body[..]);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();

    // Deserialize the JSON body
    let body_json: QueryRequestBody = serde_json::from_str(&s).unwrap();
    let (params, sql_query) = body_json.get_sql_text();
    println!("Request: {:?}", query);
    println!("Params: {:?}", params);
    println!("Query raw: {:?}", body_json.sql_text);
    println!("header_map: {:?}", headers);

    let token = extract_token(&headers).ok_or_else(|| AppError {
        message: "Missing auth token".to_string(),
    })?;

    let dbt_sessions = state.dbt_sessions.lock().await;
    let auth_data = dbt_sessions.get(token.as_str());

    if auth_data.is_none() {
        return Ok(Json(JsonResponse {
            data: None,
            success: false,
            message: Option::from("missing auth data".to_string()),
            code: Default::default(),
        }));
    }
    let (warehouse_id, database_name) = auth_data.unwrap().split_once('.').unwrap();
    let warehouse_id = Uuid::parse_str(warehouse_id).map_err(|_| AppError {
        message: "Invalid warehouse_id format".to_string(),
    })?;

    let result = state
        .control_svc
        .query_table(&warehouse_id, &database_name.to_string(), &"".to_string(), &body_json.sql_text)
        .await;

    if result.is_err() {
        let err = result.unwrap_err();
        return Ok(Json(JsonResponse {
            data: Option::from(ResponseData {
                row_type: Option::from(vec![]),
                row_set_base_64: Default::default(),
                total: Some(0),
                query_result_format: Option::from("arrow".to_string()),
                error_code: Option::from("1000".to_string()),
                sql_state: Option::from(format!("{}", err)),
            }),
            success: false,
            message: Option::from(format!("{}", err)),
            code: Default::default(),
        }));
    }

    Ok(Json(JsonResponse {
        data: Option::from(ResponseData {
            row_type: Option::from(vec![]),
            row_set_base_64: Option::from("".to_string()),
            total: Some(0),
            query_result_format: Option::from("arrow".to_string()),
            error_code: Default::default(),
            sql_state: Default::default(),
        }),
        success: true,
        message: Default::default(),
        code: Default::default(),
    }))
}

pub async fn abort() -> Result<Json<(serde_json::value::Value)>, AppError> {
    Ok(Json(json!({"success": true})))
}

pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    headers.get("authorization").and_then(|value| {
        value.to_str().ok().and_then(|auth| {
            let re = Regex::new(r#"Snowflake Token="([a-f0-9\-]+)""#).unwrap();
            re.captures(auth)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        })
    })
}
