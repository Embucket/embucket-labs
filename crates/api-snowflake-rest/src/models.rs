cfg_if::cfg_if! {
    if #[cfg(feature = "default-server")] {
        use core_executor::models::ColumnInfo as ColumnInfoModel;
        use core_executor::Row;
    } else {
        // Define simple representation for Row
        // As with external-server we won't have any dependences (especially trivial)
        type Row = Vec<serde_json::Value>;
    }
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequestQueryParams {
    #[serde(rename = "request_id")]
    pub request_id: String,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginRequestBody {
    pub data: LoginRequestData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginResponse {
    pub data: Option<LoginResponseData>,
    pub success: bool,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginResponseData {
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct LoginRequestData {
    pub client_app_id: String,
    pub client_app_version: String,
    pub svn_revision: Option<String>,
    pub account_name: String,
    pub login_name: String,
    pub client_environment: ClientEnvironment,
    pub password: String,
    pub session_parameters: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct ClientEnvironment {
    pub application: String,
    pub os: String,
    pub os_version: String,
    pub python_version: String,
    pub python_runtime: String,
    pub python_compiler: String,
    pub ocsp_mode: String,
    pub tracing: u32,
    pub login_timeout: Option<u32>,
    pub network_timeout: Option<u32>,
    pub socket_timeout: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub request_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequestBody {
    pub sql_text: String,
    pub async_exec: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AbortRequestBody {
    pub sql_text: String,
    pub request_id: Uuid, // duplicate in body, taken from snowflake connector
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseData {
    #[serde(rename = "rowtype")]
    pub row_type: Vec<ColumnInfo>,
    #[serde(rename = "rowsetBase64")]
    pub row_set_base_64: Option<String>,
    #[serde(rename = "rowset")]
    pub row_set: Option<Vec<Row>>,
    pub total: Option<u32>,
    #[serde(rename = "queryResultFormat")]
    pub query_result_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonResponse {
    pub data: Option<ResponseData>,
    pub success: bool,
    pub message: Option<String>,
    pub code: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    name: String,
    database: String,
    schema: String,
    table: String,
    nullable: bool,
    #[serde(rename = "type")]
    r#type: String,
    #[serde(rename = "byteLength")]
    byte_length: Option<i32>,
    length: Option<i32>,
    scale: Option<i32>,
    precision: Option<i32>,
    collation: Option<String>,
}

#[cfg(feature = "default-server")]
impl From<ColumnInfoModel> for ColumnInfo {
    fn from(column_info: ColumnInfoModel) -> Self {
        Self {
            name: column_info.name,
            database: column_info.database,
            schema: column_info.schema,
            table: column_info.table,
            nullable: column_info.nullable,
            r#type: column_info.r#type,
            byte_length: column_info.byte_length,
            length: column_info.length,
            scale: column_info.scale,
            precision: column_info.precision,
            collation: column_info.collation,
        }
    }
}

#[derive(Clone, Default)]
pub struct Auth {
    pub demo_user: String,
    pub demo_password: String,
}
