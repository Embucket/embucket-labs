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

use icebucket_history::{QueryRecord, QueryRecordId, WorksheetId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use validator::Validate;

pub type ExecutionContext = crate::execution::query::IceBucketQueryContext;

// Temporarily copied here
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryCreatePayload {
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryCreateResponse {
    pub id: QueryRecordId,
    pub worksheet_id: WorksheetId,
    pub query: String,
    pub result: String,
    pub duration_seconds: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueriesResponse {
    pub items: Vec<QueryRecord>,
    pub current_cursor: Option<QueryRecordId>,
    pub next_cursor: QueryRecordId,
}

#[derive(Debug, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct GetHistoryItemsParams {
    pub cursor: Option<QueryRecordId>,
    pub limit: Option<u16>,
}
