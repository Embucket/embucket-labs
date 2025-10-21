use super::error::{self as ex_error, Result};
use core_history::QueryRecordId;
use core_history::QueryStatus;
use dashmap::DashMap;
use snafu::OptionExt;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RunningQuery {
    pub query_id: QueryRecordId,
    pub request_id: Option<Uuid>,
    cancellation_token: CancellationToken,
    // user can be notified when query is finished
    tx: watch::Sender<QueryStatus>,
    rx: watch::Receiver<QueryStatus>,
}

#[derive(Debug)]
pub enum RunningQueryId {
    ByQueryId(QueryRecordId),  // (query_id)
    ByRequestId(Uuid, String), // (request_id, sql_text)
}

impl RunningQuery {
    #[must_use]
    pub fn new(query_id: QueryRecordId) -> Self {
        let (tx, rx) = watch::channel(QueryStatus::Running);
        Self {
            query_id,
            request_id: None,
            cancellation_token: CancellationToken::new(),
            tx,
            rx,
        }
    }
    #[must_use]
    pub const fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    #[tracing::instrument(
        name = "RunningQuery::notify_query_finished",
        level = "trace",
        skip(self),
        err
    )]
    pub fn notify_query_finished(
        &self,
        status: QueryStatus,
    ) -> std::result::Result<(), watch::error::SendError<QueryStatus>> {
        self.tx.send(status)
    }

    #[tracing::instrument(
        name = "RunningQuery::recv_query_finished",
        level = "trace",
        skip(self),
        err
    )]
    pub async fn recv_query_finished(
        &mut self,
    ) -> std::result::Result<QueryStatus, watch::error::RecvError> {
        // use loop here to bypass default query status we posted at init
        // it should not go to the actual loop and should resolve as soon as results are ready
        loop {
            self.rx.changed().await?;
            let status = *self.rx.borrow();
            if status != QueryStatus::Running {
                break Ok(status);
            }
        }
    }
}

pub struct RunningQueriesRegistry {
    // <query_id, RunningQuery>
    queries: Arc<DashMap<i64, RunningQuery>>,
    // <request_id, QueryRecordId> To associate request_id with query_id
    requests_ids: Arc<DashMap<Uuid, QueryRecordId>>,
}

impl Default for RunningQueriesRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RunningQueriesRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queries: Arc::new(DashMap::new()),
            requests_ids: Arc::new(DashMap::new()),
        }
    }
}

// RunningQueries interface allows cancel queries by query_id or request_id
pub trait RunningQueries: Send + Sync {
    fn add(&self, running_query: RunningQuery) -> CancellationToken;
    fn remove(&self, running_query_id: RunningQueryId) -> Result<RunningQuery>;
    fn abort(&self, abort_query: RunningQueryId) -> Result<()>;
    fn is_running(&self, running_query_id: RunningQueryId) -> bool;
    fn count(&self) -> usize;
    fn get(&self, running_query_id: RunningQueryId) -> Result<RunningQuery>;
}

impl RunningQueries for RunningQueriesRegistry {
    #[tracing::instrument(name = "RunningQueriesRegistry::add", level = "trace", skip(self))]
    fn add(&self, running_query: RunningQuery) -> CancellationToken {
        let cancellation_token = running_query.cancellation_token.clone();

        // map query_id to request_id
        if let Some(request_id) = running_query.request_id {
            self.requests_ids.insert(request_id, running_query.query_id);
        }

        // map RunningQuery to query_id
        self.queries
            .insert(running_query.query_id.as_i64(), running_query);

        cancellation_token
    }

    #[tracing::instrument(
        name = "RunningQueriesRegistry::remove",
        level = "trace",
        skip(self),
        err
    )]
    fn remove(&self, running_query_id: RunningQueryId) -> Result<RunningQuery> {
        let query_id = self.get(running_query_id)?.query_id;
        if let Some((_query_id, running_query)) = self.queries.remove(&query_id.as_i64()) {
            Ok(running_query)
        } else {
            ex_error::QueryIsntRunningSnafu { query_id }.fail()
        }
    }

    fn get(&self, running_query_id: RunningQueryId) -> Result<RunningQuery> {
        let running_query = match running_query_id {
            RunningQueryId::ByQueryId(query_id) => self
                .queries
                .get(&query_id.as_i64())
                .context(ex_error::QueryIsntRunningSnafu { query_id })?,
            RunningQueryId::ByRequestId(request_id, _sql_text) => {
                let query_id = self
                    .requests_ids
                    .get(&request_id)
                    .context(ex_error::QueryByRequestIdIsntRunningSnafu { request_id })?;
                self.queries
                    .get(&query_id.as_i64())
                    .context(ex_error::QueryIsntRunningSnafu {
                        query_id: query_id.as_i64(),
                    })?
            }
        };
        // Can't return reference to RunningQuery, but it quite small so clone it
        Ok(running_query.clone())
    }

    #[tracing::instrument(
        name = "RunningQueriesRegistry::abort",
        level = "trace",
        skip(self),
        fields(running_queries_count = self.count()),
        err
    )]
    fn abort(&self, abort_query: RunningQueryId) -> Result<()> {
        // Two phase mechanism:
        // 1 - cancel query using cancellation_token
        // 2 - ExecutionService removes RunningQuery from RunningQueriesRegistry
        match abort_query {
            RunningQueryId::ByQueryId(query_id) => {
                let running_query = self
                    .queries
                    .get(&query_id.as_i64())
                    .context(ex_error::QueryIsntRunningSnafu { query_id })?;
                running_query.cancel();
            }
            // sql_text is not realy used for aborting query. Should it be checked too?
            RunningQueryId::ByRequestId(request_id, _sql_text) => {
                let query_record_id = self
                    .requests_ids
                    .get(&request_id)
                    .context(ex_error::QueryByRequestIdIsntRunningSnafu { request_id })?;
                self.abort(RunningQueryId::ByQueryId(*query_record_id))?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "RunningQueriesRegistry::exists",
        level = "trace",
        skip(self),
        ret
    )]
    fn is_running(&self, running_query_id: RunningQueryId) -> bool {
        self.get(running_query_id).is_ok()
    }

    fn count(&self) -> usize {
        self.queries.len()
    }
}
