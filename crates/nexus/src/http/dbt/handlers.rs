use crate::error::AppError;
use crate::http::dbt::schemas::{
    JsonResponse, LoginData, LoginRequestBody, LoginRequestQuery, LoginResponse, QueryRequest
    , ResponseData,
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

pub async fn login(
    State(state): State<AppState>,
    Query(query): Query<LoginRequestQuery>,
    body: Bytes,
) -> Json<LoginResponse> {
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
        .await
        .map_err(|e| {
            Json(LoginResponse {
                data: None,
                success: false,
                message: None,
            })
        })
        .unwrap()
        .into_iter()
        .filter(|w| w.name == query.warehouse)
    {
        state.dbt_sessions.lock().await.insert(
            token.clone(),
            format!("{}.{}", warehouse.id, query.database_name),
        );
    }
    Json(LoginResponse {
        data: Option::from(LoginData { token }),
        success: true,
        message: Option::from("successfully executed".to_string()),
    })
}

pub async fn query(
    State(state): State<AppState>,
    Query(query): Query<QueryRequest>,
    headers: HeaderMap,
    body: Bytes,
) -> Json<JsonResponse> {
    // // Decompress the gzip-encoded body
    // let mut d = GzDecoder::new(&body[..]);
    // let mut s = String::new();
    // d.read_to_string(&mut s).unwrap();
    //
    // // Deserialize the JSON body
    // let body_json: QueryRequestBody = serde_json::from_str(&s).unwrap();
    // let (params, sql_query) = body_json.get_sql_text();
    // println!("Request: {:?}", query);
    // println!("Params: {:?}", params);
    // println!("Query raw: {:?}", body_json.sql_text);
    // println!("header_map: {:?}", headers);
    //
    // let token = match extract_token(&headers) {
    //     Some(token) => token,
    //     None => {
    //         return Json(JsonResponse::bad_default("missing auth token".to_string()));
    //     }
    // };
    //
    // let dbt_sessions = state.dbt_sessions.lock().await;
    // let auth_data = dbt_sessions.get(token.as_str());
    //
    // if auth_data.is_none() {
    //     return Json(JsonResponse::bad_default("missing session".to_string()));
    // }
    //
    // let (warehouse_id, database_name) = auth_data.unwrap().split_once('.').unwrap();
    // let warehouse_id = match Uuid::parse_str(warehouse_id) {
    //     Ok(w_id) => w_id,
    //     Err(e) => {
    //         return Json(JsonResponse::bad_default(format!(
    //             "{}: {}",
    //             "invalid warehouse_id format".to_string(),
    //             e
    //         )));
    //     }
    // };

    let warehouse = state
        .control_svc
        .list_warehouses()
        .await
        .unwrap()
        .into_iter()
        .filter(|w| w.name == "embucket").map(|w| w.id).collect()[0];

    let (result, columns) = state
        .control_svc
        .query_dbt(
            &warehouse,
            &"datasets".to_string(),
            //&database_name.to_string(),
            &"".to_string(),
            &"SELECT * FROM embucket.datasets.variant_insert_2".to_string(),
        )
        .await
        .map_err(|e| {
            Json(JsonResponse::bad_default(format!("{}", e)))
        })
        .unwrap();

    Json(JsonResponse {
        data: Option::from(ResponseData {
            row_type: columns.into_iter().map(|c| c.into()).collect(),
            row_set_base_64: Option::from("".to_string()),
            total: Some(1),
            query_result_format: Option::from("arrow".to_string()),
            error_code: None,
            sql_state: Default::default(),
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
        code: Some(format!("{:06}", 200)),
    })
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
