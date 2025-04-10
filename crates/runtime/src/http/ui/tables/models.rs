// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableStatisticsResponse {
    #[serde(flatten)]
    pub(crate) data: TableStatistics,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableStatistics {
    pub(crate) name: String,
    pub(crate) total_rows: i64,
    pub(crate) total_bytes: i64,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableColumnsInfoResponse {
    pub(crate) items: Vec<TableColumnInfo>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableColumnInfo {
    pub(crate) name: String,
    pub(crate) r#type: String,
    pub(crate) description: String,
    pub(crate) nullable: String,
    pub(crate) default: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TablePreviewDataResponse {
    pub(crate) items: Vec<TablePreviewDataColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TablePreviewDataColumn {
    pub(crate) name: String,
    pub(crate) rows: Vec<TablePreviewDataRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TablePreviewDataRow {
    pub(crate) data: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub(crate) struct TablePreviewDataParameters {
    pub(crate) offset: Option<u32>,
    pub(crate) limit: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableUploadPayload {
    #[schema(format = "binary")]
    pub upload_file: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableUploadResponse {
    pub(crate) count: usize,
    pub(crate) duration_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CsvParameters {}

// path – Path to the CSV file
// schema – An optional schema representing the CSV files. If None, the CSV reader will try to infer it based on data in file.
// has_header – Whether the CSV file have a header. If schema inference is run on a file with no headers, default column names are created.
// delimiter – An optional column delimiter.
// schema_infer_max_records – Maximum number of rows to read from CSV files for schema inference if needed.
// file_extension – File extension; only files with this extension are selected for data input.
// table_partition_cols – Partition columns.
// file_compression_type – File compression type.
