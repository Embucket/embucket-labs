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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Running,
    Successful,
    Failed,
}

pub type QueryRecordId = i64;

// QueryRecord struct is used for storing QueryRecord History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    // Returns a key with inverted id for descending order
    #[must_use]
    pub fn get_key(id: QueryRecordId) -> Bytes {
        Bytes::from(format!("/qh/{id}"))
    }

    #[allow(clippy::expect_used)]
    fn inverted_id(id: QueryRecordId) -> QueryRecordId {
        let inverted_str: String = id.to_string().chars().map(Self::invert_digit).collect();

        inverted_str
            .parse()
            .expect("Failed to parse inverted QueryRecordId")
    }

    const fn invert_digit(digit: char) -> char {
        match digit {
            '0' => '9',
            '1' => '8',
            '2' => '7',
            '3' => '6',
            '4' => '5',
            '5' => '4',
            '6' => '3',
            '7' => '2',
            '8' => '1',
            '9' => '0',
            _ => digit, // Return the digit unchanged if it's not a number (just in case)
        }
    }
}

impl QueryRecordActions for QueryRecord {
    #[must_use]
    fn query_start(query: &str, worksheet_id: Option<WorksheetId>) -> Self {
        let start_time = Utc::now();
        Self {
            id: Self::inverted_id(start_time.timestamp_millis()),
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
        self.id
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.id)
    }
}
