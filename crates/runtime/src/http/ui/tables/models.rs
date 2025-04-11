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
use datafusion::arrow::csv::reader::Format;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableStatisticsResponse {
    #[serde(flatten)]
    pub data: TableStatistics,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableStatistics {
    pub name: String,
    pub total_rows: i64,
    pub total_bytes: i64,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableColumnsInfoResponse {
    pub items: Vec<TableColumnInfo>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableColumnInfo {
    pub name: String,
    pub r#type: String,
    pub description: String,
    pub nullable: String,
    pub default: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TablePreviewDataResponse {
    pub items: Vec<TablePreviewDataColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TablePreviewDataColumn {
    pub name: String,
    pub rows: Vec<TablePreviewDataRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TablePreviewDataRow {
    pub data: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct TablePreviewDataParameters {
    pub offset: Option<u32>,
    pub limit: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableUploadPayload {
    #[schema(format = "binary")]
    pub upload_file: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableUploadResponse {
    pub count: usize,
    pub duration_ms: u128,
}

// header – Whether the CSV file have a header, defaults to `false`
// delimiter – An optional column delimiter, defaults to comma `','`
// escape - escape character, defaults to `None`
// quote - a custom quote character, defaults to double quote `'"'`
// terminator - a custom terminator character, defaults to CRLF
// comment - a comment character, defaults to `None`
#[derive(Debug, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct UploadParameters {
    pub header: Option<bool>,
    pub delimiter: Option<u8>,
    pub escape: Option<u8>,
    pub quote: Option<u8>,
    pub terminator: Option<u8>,
    pub comment: Option<u8>,
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
#[allow(clippy::from_over_into)]
impl Into<Format> for UploadParameters {
    fn into(self) -> Format {
        let format = Format::default();

        let format = if let Some(header) = self.header {
            format.with_header(header)
        } else {
            format
        };

        let format = if let Some(delimiter) = self.delimiter {
            format.with_delimiter(delimiter)
        } else {
            format
        };

        let format = if let Some(escape) = self.escape {
            format.with_escape(escape)
        } else {
            format
        };

        let format = if let Some(quote) = self.quote {
            format.with_quote(quote)
        } else {
            format
        };

        let format = if let Some(terminator) = self.terminator {
            format.with_terminator(terminator)
        } else {
            format
        };

        if let Some(comment) = self.comment {
            format.with_comment(comment)
        } else {
            format
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TablesResponse {
    pub items: Vec<Table>,
    pub current_cursor: Option<String>,
    pub next_cursor: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    pub name: String,
    pub r#type: String,
    pub owner: String,
    pub total_rows: i64,
    pub total_bytes: i64,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}
#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct TablesParameters {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub search: Option<String>,
}
