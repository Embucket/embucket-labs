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
    #[serde(rename = "rowType")]
    pub row_type: Option<Vec<RowType>>,
    #[serde(rename = "rowSetBase64")]
    pub row_set_base_64: Option<String>,
    pub total: Option<u32>,
    pub query_result_format: Option<String>,
    pub error_code: Option<String>,
    pub sql_state: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RowType {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonResponse {
    pub data: Option<ResponseData>,
    pub success: bool,
    pub message: Option<String>,
    pub code: Option<i32>,

}
