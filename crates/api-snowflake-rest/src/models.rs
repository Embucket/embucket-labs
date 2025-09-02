use super::error::{self as api_snowflake_rest_error, Result};
use core_executor::models::ColumnInfo as ColumnInfoModel;
use core_executor::utils::DataSerializationFormat;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;

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
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequestBody {
    pub sql_text: String,
    pub async_exec: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseData {
    #[serde(rename = "rowtype")]
    pub row_type: Vec<ColumnInfo>,
    #[serde(rename = "rowsetBase64")]
    pub row_set_base_64: Option<String>,
    #[serde(rename = "rowset")]
    pub row_set: Option<Vec<Vec<serde_json::Value>>>,
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

impl ResponseData {
    pub fn rows_to_vec(json_rows_string: &str) -> Result<Vec<Vec<serde_json::Value>>> {
        let json_array: Vec<IndexMap<String, serde_json::Value>> =
            serde_json::from_str(json_rows_string)
                .context(api_snowflake_rest_error::RowParseSnafu)?;
        Ok(json_array
            .into_iter()
            .map(|obj| obj.values().cloned().collect())
            .collect())
    }
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

#[derive(Clone, Default)]
pub struct Config {
    pub auth: Auth,
    pub dbt_serialization_format: DataSerializationFormat,
}

impl Config {
    pub fn new(data_format: &str) -> std::result::Result<Self, strum::ParseError> {
        Ok(Self {
            dbt_serialization_format: DataSerializationFormat::try_from(data_format)?,
            ..Self::default()
        })
    }
    #[must_use]
    pub fn with_demo_credentials(mut self, demo_user: String, demo_password: String) -> Self {
        self.auth = Auth {
            demo_user,
            demo_password,
        };
        self
    }
}
