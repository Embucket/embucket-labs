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
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
pub enum QueryStatus {
    Ok,
    Error,
}

pub type QueryRecordId = i64;

// QueryRecord struct is used for storing QueryRecord History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueryRecord {
    pub id: QueryRecordId,
    pub worksheet_id: WorksheetId,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub result_count: i64,
    pub result: Option<String>,
    pub status: QueryStatus,
    pub error: Option<String>,
}

impl QueryRecord {
    #[must_use]
    pub fn get_key(worksheet_id: WorksheetId, id: QueryRecordId) -> Bytes {
        Bytes::from(format!("/qh/{worksheet_id}/{id}"))
    }

    #[must_use]
    pub fn query_start(
        worksheet_id: WorksheetId,
        query: &str,
        start_time: Option<DateTime<Utc>>,
    ) -> Self {
        let start_time = start_time.unwrap_or_else(Utc::now);
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
            status: QueryStatus::Ok,
            error: None,
        }
    }

    pub fn query_finished(
        &mut self,
        result_count: i64,
        result: Option<String>,
        end_time: Option<DateTime<Utc>>,
    ) {
        self.result_count = result_count;
        self.result = result;
        self.end_time = Utc::now();
        self.duration_ms = self
            .end_time
            .signed_duration_since(self.start_time)
            .num_milliseconds();
        if let Some(end_time) = end_time {
            self.end_time = end_time;
        }
    }

    pub fn query_finished_with_error(&mut self, error: String) {
        self.query_finished(0, None, None);
        self.status = QueryStatus::Error;
        self.error = Some(error);
    }
}

impl IterableEntity for QueryRecord {
    type Cursor = i64;

    fn cursor(&self) -> Self::Cursor {
        self.start_time.timestamp_millis()
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.worksheet_id, self.id)
    }
}
