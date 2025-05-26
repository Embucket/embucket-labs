// --- From entities/worksheet.rs ---
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
// Assuming core_utils will be a dependency of core-traits for IterableEntity
// For now, this path will be `core_utils::iterable::IterableEntity`
// If IterableEntity is also moved to core-traits, this path would change.
use core_utils::iterable::IterableEntity;


pub type WorksheetId = i64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Worksheet {
    pub id: WorksheetId,
    pub name: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Worksheet {
    #[must_use]
    pub fn get_key(id: WorksheetId) -> Bytes {
        Bytes::from(format!("/ws/{id}"))
    }

    #[must_use]
    pub fn new(name: String, content: String) -> Self {
        let created_at = Utc::now();
        let id = created_at.timestamp_millis();
        Self {
            id,
            name,
            content,
            created_at,
            updated_at: created_at,
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_content(&mut self, content: String) {
        self.content = content;
    }

    pub fn set_updated_at(&mut self, updated_at: Option<DateTime<Utc>>) {
        self.updated_at = updated_at.unwrap_or_else(Utc::now);
    }
}

impl IterableEntity for Worksheet {
    type Cursor = WorksheetId;

    fn cursor(&self) -> Self::Cursor {
        self.id
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.id)
    }
}


// --- From entities/query.rs ---
// WorksheetId is already defined above
// Bytes, DateTime, Utc, IterableEntity, Serialize, Deserialize already imported
#[cfg(test)]
use mockall::automock;


#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Running,
    Successful,
    Failed,
}

pub type QueryRecordId = i64;

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

#[cfg_attr(test, automock(type Error=String;))] // Added mockall Error type for clarity
pub trait ExecutionQueryRecord {
    fn query_id(&self) -> QueryRecordId;
    fn query_start(query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord;
    fn query_finished(&mut self, result_count: i64, result: Option<String>);
    fn query_finished_with_error(&mut self, error: String);
}

impl QueryRecord {
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
            '0' => '9', '1' => '8', '2' => '7', '3' => '6', '4' => '5',
            '5' => '4', '6' => '3', '7' => '2', '8' => '1', '9' => '0',
            _ => digit,
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
            status: QueryStatus::Successful, // Original was Successful, keeping it
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


// --- From history_store.rs ---
use async_trait::async_trait;
use snafu::Snafu;
// Assuming core_utils::Error will be a dependency.
// Assuming slatedb::SlateDBError will be a dependency.
// Assuming serde_json::Error will be a dependency.

#[derive(Snafu, Debug)]
pub enum WorksheetsStoreError {
    #[snafu(display("Error using key: {source}"))]
    BadKey { source: std::str::Utf8Error },

    #[snafu(display("Error adding worksheet: {source}"))]
    WorksheetAdd { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error getting worksheet: {source}"))]
    WorksheetGet { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error getting worksheets: {source}"))]
    WorksheetsList { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error deleting worksheet: {source}"))]
    WorksheetDelete { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error updating worksheet: {source}"))]
    WorksheetUpdate { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error adding query record: {source}"))]
    QueryAdd { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Can't locate query record by key: {key}"))]
    QueryNotFound { key: String },

    #[snafu(display("Error adding query record reference: {source}"))]
    QueryReferenceAdd { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error getting query history: {source}"))]
    QueryGet { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Can't locate worksheet by key: {message}"))]
    WorksheetNotFound { message: String },

    #[snafu(display("Bad query record reference key: {key}"))]
    QueryReferenceKey { key: String },

    #[snafu(display("Error getting worksheet queries: {source}"))]
    GetWorksheetQueries { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Error adding query inverted key: {source}"))]
    QueryInvertedKeyAdd { source: core_utils::Error }, // core_utils::Error

    #[snafu(display("Query item seek error: {source}"))]
    Seek { source: slatedb::SlateDBError }, // slatedb::SlateDBError

    #[snafu(display("Deserialize error: {source}"))]
    DeserializeValue { source: serde_json::Error }, // serde_json::Error
}

pub type WorksheetsStoreResult<T> = std::result::Result<T, WorksheetsStoreError>;

#[derive(Default, Clone, Debug)]
pub enum SortOrder { // This was not explicitly requested but seems closely related
    Ascending,
    #[default]
    Descending,
}

#[derive(Default, Debug, Clone)] // Added Clone for consistency if needed
pub struct GetQueries {
    pub worksheet_id: Option<WorksheetId>,
    pub sql_text: Option<String>,
    pub min_duration_ms: Option<i64>,
    pub cursor: Option<QueryRecordId>,
    pub limit: Option<u16>,
}

impl GetQueries {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn with_worksheet_id(mut self, worksheet_id: WorksheetId) -> Self {
        self.worksheet_id = Some(worksheet_id);
        self
    }

    #[must_use]
    pub fn with_sql_text(mut self, sql_text: String) -> Self {
        self.sql_text = Some(sql_text);
        self
    }

    #[must_use]
    pub const fn with_min_duration_ms(mut self, min_duration_ms: i64) -> Self {
        self.min_duration_ms = Some(min_duration_ms);
        self
    }

    #[must_use]
    pub const fn with_cursor(mut self, cursor: QueryRecordId) -> Self {
        self.cursor = Some(cursor);
        self
    }

    #[must_use]
    pub const fn with_limit(mut self, limit: u16) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[async_trait]
pub trait WorksheetsStore: std::fmt::Debug + Send + Sync {
    async fn add_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<Worksheet>;
    async fn get_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<Worksheet>;
    async fn delete_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<()>;
    async fn update_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<()>; // removed mut
    async fn get_worksheets(&self) -> WorksheetsStoreResult<Vec<Worksheet>>;

    async fn add_query(&self, item: &QueryRecord) -> WorksheetsStoreResult<()>;
    async fn get_query(&self, id: QueryRecordId) -> WorksheetsStoreResult<QueryRecord>;
    async fn get_queries(&self, params: GetQueries) -> WorksheetsStoreResult<Vec<QueryRecord>>;
}
