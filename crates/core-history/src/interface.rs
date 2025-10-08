use crate::errors::Result;
use crate::{QueryRecord, QueryRecordId, QueryStatus, Worksheet, WorksheetId};
use async_trait::async_trait;


#[derive(Debug, Clone)]
pub struct QueryResultError {
    // additional error status like: cancelled, timeout, etc
    pub status: QueryStatus,
    pub message: String,
    pub diagnostic_message: String,
}
impl std::fmt::Display for QueryResultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // do not output status, it is just an internal context
        write!(
            f,
            "QueryResultError: {} | Diagnostic: {}",
            self.message, self.diagnostic_message
        )
    }
}

impl std::error::Error for QueryResultError {}

#[derive(Default, Debug)]
pub struct GetQueriesParams {
    pub worksheet_id: Option<WorksheetId>,
    pub sql_text: Option<String>,     // filter by SQL Text
    pub min_duration_ms: Option<i64>, // filter Duration greater than
    pub cursor: Option<QueryRecordId>,
    pub limit: Option<u16>,
}

impl GetQueriesParams {
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


#[mockall::automock]
#[async_trait]
pub trait HistoryStore: std::fmt::Debug + Send + Sync {
    async fn add_worksheet(&self, worksheet: Worksheet) -> Result<Worksheet>;
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet>;
    async fn update_worksheet(&self, worksheet: Worksheet) -> Result<()>;
    async fn delete_worksheet(&self, id: WorksheetId) -> Result<()>;
    async fn get_worksheets(&self) -> Result<Vec<Worksheet>>;
    async fn add_query(&self, item: &QueryRecord) -> Result<()>;
    async fn update_query(&self, item: &QueryRecord) -> Result<()>;
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord>;
    async fn get_queries(&self, params: GetQueriesParams) -> Result<Vec<QueryRecord>>;
    fn query_record(&self, query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord;
    async fn save_query_record(&self, query_record: &mut QueryRecord);
}