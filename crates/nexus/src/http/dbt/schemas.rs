use control_plane::models::ColumnInfo as ColumnInfoModel;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoginRequestQuery {
    #[serde(rename = "request_id")]
    pub request_id: String,
    #[serde(rename = "databaseName")]
    pub database_name: String,
    #[serde(rename = "schemaName")]
    pub schema_name: String,
    #[serde(rename = "warehouse")]
    pub warehouse: String,
    #[serde(rename = "roleName")]
    pub role_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoginRequestBody {
    pub data: ClientData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoginResponse {
    pub data: Option<LoginData>,
    pub success: bool,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoginData {
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct ClientData {
    pub client_app_id: String,
    pub client_app_version: String,
    pub svn_revision: Option<String>,
    pub account_name: String,
    pub login_name: String,
    pub client_environment: ClientEnvironment,
    pub password: String,
    pub session_parameters: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    #[serde(rename = "requestId")]
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryRequestBody {
    #[serde(rename = "sqlText")]
    pub sql_text: String,
}
impl QueryRequestBody {
    pub fn get_sql_text(&self) -> (HashMap<String, String>, String) {
        let sql_text = self.sql_text.clone();
        let comment_end = sql_text.find("*/").map(|i| i + 2).unwrap_or(0);
        let metadata_str = &sql_text[2..comment_end - 2].trim();
        let metadata: HashMap<String, String> = serde_json::from_str(metadata_str).unwrap();
        let query = sql_text[comment_end..].trim().to_string();
        (metadata, query)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ResponseData {
    #[serde(rename = "rowtype")]
    pub row_type: Vec<ColumnInfo>,
    #[serde(rename = "rowsetBase64")]
    pub row_set_base_64: Option<String>,
    pub total: Option<u32>,
    #[serde(rename = "queryResultFormat")]
    pub query_result_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonResponse {
    pub data: Option<ResponseData>,
    pub success: bool,
    pub message: Option<String>,
    pub code: Option<String>,
}

impl JsonResponse {
    pub(crate) fn bad_default(msg: String) -> Self {
        Self {
            data: None,
            success: false,
            message: Option::from(msg),
            code: Some(format!("{:06}", 422)),
        }
    }
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
