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

use icebucket_history::{QueryHistoryId, QueryItem, WorksheetId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use validator::Validate;

// Temporarily copied here
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryPayload {
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}

impl QueryPayload {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(query: String) -> Self {
        Self {
            query,
            context: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub id: QueryHistoryId,
    pub worksheet_id: WorksheetId,
    pub query: String,
    pub result: String,
    pub duration_seconds: f32,
}

impl QueryResponse {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(
        id: QueryHistoryId,
        worksheet_id: WorksheetId,
        query: String,
        result: String,
        duration_seconds: f32,
    ) -> Self {
        Self {
            id,
            worksheet_id,
            query,
            result,
            duration_seconds,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueriesResponse {
    pub items: Vec<QueryItem>,
    pub current_cursor: Option<QueryHistoryId>,
    pub next_cursor: QueryHistoryId,
}

#[derive(Debug, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct GetHistoryItemsParams {
    pub cursor: Option<QueryHistoryId>,
    pub limit: Option<u16>,
}
