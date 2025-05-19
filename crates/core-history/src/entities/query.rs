use crate::WorksheetId;
use api_structs::query::{QueryRecord as QueryRecordRest, QueryStatus};
use api_structs::result_set::{ResultSet, ResultSetError};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use core_utils::iterable::IterableEntity;
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};

pub type QueryRecordId = i64;

#[cfg_attr(test, automock)]
pub trait ExecutionQueryRecord {
    fn query_id(&self) -> QueryRecordId;

    fn query_start(query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord;

    fn query_finished(&mut self, result_count: i64, result: Option<String>);

    fn query_finished_with_error(&mut self, error: String);
}

// QueryRecord struct is used for storing QueryRecord History result and also used in http response
// It's bit different from api_structs::query::QueryRecord, migrate
// Migrate with caution to api_structs::query::QueryRecord as result_set differs,
// worksheet_id  serializes with empty values when stored
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

impl TryInto<QueryRecordRest> for QueryRecord {
    type Error = ResultSetError;

    fn try_into(self) -> Result<QueryRecordRest, Self::Error> {
        let query_result = self.result.unwrap_or_default();
        let query_error = self.error.unwrap_or_default();
        let result_set = if query_result.is_empty() {
            ResultSet {
                rows: vec![],
                columns: vec![],
            }
        } else {
            ResultSet::try_from(query_result.as_str())?
        };

        Ok(QueryRecordRest {
            id: self.id,
            worksheet_id: self.worksheet_id,
            query: self.query,
            start_time: self.start_time,
            end_time: self.end_time,
            duration_ms: self.duration_ms,
            result_count: self.result_count,
            status: self.status,
            result: result_set,
            error: query_error,
        })
    }
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

impl ExecutionQueryRecord for QueryRecord {
    fn query_id(&self) -> QueryRecordId {
        self.id
    }

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
