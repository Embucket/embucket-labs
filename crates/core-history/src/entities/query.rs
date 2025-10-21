use crate::{QueryRecordId, WorksheetId};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use core_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
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

#[derive(Debug)]
pub struct ParseQueryStatusError;

impl std::fmt::Display for ParseQueryStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid query status")
    }
}

impl std::error::Error for ParseQueryStatusError {}

impl FromStr for QueryStatus {
    type Err = ParseQueryStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Running" => Ok(Self::Running),
            "Successful" => Ok(Self::Successful),
            "Failed" => Ok(Self::Failed),
            "Canceled" => Ok(Self::Canceled),
            "TimedOut" => Ok(Self::TimedOut),
            _ => Err(ParseQueryStatusError),
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
    pub result_id: Option<String>,
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
            id: Self::inverted_id(QueryRecordId(start_time.timestamp_micros())),
            worksheet_id,
            query: String::from(query),
            start_time,
            end_time: start_time,
            duration_ms: 0,
            result_count: 0,
            result_id: None,
            status: QueryStatus::Running,
            error: None,
            diagnostic_error: None,
        }
    }

    #[must_use]
    pub const fn query_id(&self) -> QueryRecordId {
        self.id
    }

    pub fn finished_with_status(&mut self, status: QueryStatus, result_count: i64) {
        self.result_count = result_count;
        self.status = status;
        self.end_time = Utc::now();
        self.duration_ms = self
            .end_time
            .signed_duration_since(self.start_time)
            .num_milliseconds();
    }

    pub fn finished_with_error(&mut self, error: &crate::QueryResultError) {
        self.finished_with_status(error.status, 0);
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
