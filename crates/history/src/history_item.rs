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

use chrono::{DateTime, Utc};
use icebucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

// HistoryItem struct is used for storing Query History result and also used in http response
#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryItem {
    pub id: Uuid,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub result_count: i64,
    pub status_code: u16,
    pub error: Option<String>,
}

impl HistoryItem {
    #[must_use]
    pub fn before_started(
        query: &str,
        id: Option<Uuid>,
        start_time: Option<DateTime<Utc>>,
    ) -> Self {
        let start_time = start_time.unwrap_or_else(Utc::now);
        Self {
            id: id.unwrap_or_else(Uuid::new_v4),
            query: String::from(query),
            start_time,
            end_time: start_time,
            status_code: 200,
            duration_ms: 0,
            result_count: 0,
            error: None,
        }
    }

    pub fn set_finished(&mut self, result_count: i64, end_time: Option<DateTime<Utc>>) {
        self.result_count = result_count;
        self.end_time = Utc::now();
        self.duration_ms = self
            .end_time
            .signed_duration_since(self.start_time)
            .num_milliseconds();
        if let Some(end_time) = end_time {
            self.end_time = end_time;
        }
    }

    pub fn set_finished_with_error(&mut self, error: String, error_code: u16) {
        self.set_finished(0, None);
        self.error = Some(error);
        self.status_code = error_code;
    }
}

impl IterableEntity for HistoryItem {
    type Cursor = i64;
    const PREFIX: &[u8] = b"hi.";

    fn cursor(&self) -> Self::Cursor {
        self.start_time.timestamp_nanos_opt().unwrap_or(0)
    }

    fn next_cursor(&self) -> Self::Cursor {
        self.cursor() + 1
    }
}
