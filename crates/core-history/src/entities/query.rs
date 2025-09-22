use crate::{QueryRecordId, QueryResultError, ResultSet, WorksheetId};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use core_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Running,
    Successful,
    Failed,
    Canceled,
    TimedOut,
}

impl Display for QueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "Running"),
            Self::Successful => write!(f, "Successful"),
            Self::Failed => write!(f, "Failed"),
            Self::Canceled => write!(f, "Canceled"),
            Self::TimedOut => write!(f, "TimedOut"),
        }
    }
}

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
    pub diagnostic_error: Option<String>,
}

impl QueryRecord {
    // When created - it's Running by default
    #[must_use]
    pub fn new(query: &str, worksheet_id: Option<WorksheetId>) -> Self {
        let start_time = Utc::now();
        Self {
            id: Self::inverted_id(QueryRecordId(start_time.timestamp_millis())),
            worksheet_id,
            query: String::from(query),
            start_time,
            end_time: start_time,
            duration_ms: 0,
            result_count: 0,
            result: None,
            status: QueryStatus::Running,
            error: None,
            diagnostic_error: None,
        }
    }

    #[must_use]
    pub const fn query_id(&self) -> QueryRecordId {
        self.id
    }

    pub const fn set_status(&mut self, status: QueryStatus) {
        self.status = status;
    }

    // This takes result by reference, since it just serialize it, so can't be consumed
    pub fn set_result(&mut self, result: &Result<ResultSet, QueryResultError>) {
        match result {
            Ok(result_set) => match serde_json::to_string(&result_set) {
                Ok(encoded_res) => {
                    let result_count = i64::try_from(result_set.rows.len()).unwrap_or(0);
                    self.finished(result_count, Some(encoded_res));
                }
                // serde error
                // Following error is created right here, so ownership could be transferred into
                Err(err) => self.finished_with_error(&QueryResultError {
                    status: QueryStatus::Failed,
                    message: err.to_string(),
                    diagnostic_message: format!("{err:?}"),
                }),
            },
            // Following error is received from outside, so can be used just as reference
            Err(execution_err) => self.finished_with_error(execution_err),
        }
    }

    pub fn finished(&mut self, result_count: i64, result: Option<String>) {
        self.result_count = result_count;
        self.result = result;
        self.end_time = Utc::now();
        self.duration_ms = self
            .end_time
            .signed_duration_since(self.start_time)
            .num_milliseconds();
    }

    pub fn finished_with_error(&mut self, error: &crate::QueryResultError) {
        self.finished(0, None);
        // Copy all data
        // Consider transfering ownership if redesign is done
        self.status = error.status.clone();
        self.error = Some(error.message.clone());
        self.diagnostic_error = Some(error.diagnostic_message.clone());
    }

    // Returns a key with inverted id for descending order
    #[must_use]
    pub fn get_key(id: i64) -> Bytes {
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

impl IterableEntity for QueryRecord {
    type Cursor = i64;

    fn cursor(&self) -> Self::Cursor {
        self.id.into()
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.cursor())
    }
}
