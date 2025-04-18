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

use crate::WorksheetId;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use embucket_utils::iterable::IterableEntity;
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Running,
    Successful,
    Failed,
}

pub type QueryRecordId = i64;

// QueryRecord struct is used for storing QueryRecord History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueryRecord {
    pub id: QueryRecordId,
    pub worksheet_id: Option<WorksheetId>,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub result_count: i64,
    pub result: Option<String>,
    pub status: QueryStatus,
    pub error: Option<String>,
}

#[cfg_attr(test, automock)]
pub trait QueryRecordActions {
    fn query_start(query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord;

    fn query_finished(&mut self, result_count: i64, result: Option<String>);

    fn query_finished_with_error(&mut self, error: String);
}

impl QueryRecord {
    #[must_use]
    pub fn get_key(id: QueryRecordId) -> Bytes {
        Bytes::from(format!("/qh/{id}"))
    }
}

impl QueryRecordActions for QueryRecord {
    #[must_use]
    fn query_start(query: &str, worksheet_id: Option<WorksheetId>) -> Self {
        let start_time = Utc::now();
        // id, start_time have the same value
        Self {
            id: start_time.timestamp_millis(),
            worksheet_id,
            query: String::from(query),
            start_time,
            end_time: start_time,
            duration_ms: 0,
            result_count: 0,
            result: None,
            status: QueryStatus::Successful,
            error: None,
        }
    }

    fn query_finished(&mut self, result_count: i64, result: Option<String>) {
        self.result_count = result_count;
        self.result = result;
        self.end_time = Utc::now();
        self.duration_ms = self
            .end_time
            .signed_duration_since(self.start_time)
            .num_milliseconds();
    }

    fn query_finished_with_error(&mut self, error: String) {
        self.query_finished(0, None);
        self.status = QueryStatus::Failed;
        self.error = Some(error);
    }
}

impl IterableEntity for QueryRecord {
    type Cursor = i64;

    fn cursor(&self) -> Self::Cursor {
        self.start_time.timestamp_millis()
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.id)
    }
}
