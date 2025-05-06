use crate::{QueryRecord, QueryRecordId, QueryRecordReference, Worksheet, WorksheetId, Token, TokenId};
use async_trait::async_trait;
use embucket_utils::iterable::IterableCursor;
use embucket_utils::{Db, Error};
use futures::future::join_all;
use serde_json::de;
use slatedb::DbIterator;
use slatedb::SlateDBError;
use snafu::{ResultExt, Snafu};
use std::str;
use std::sync::Arc;

pub struct SlateDBWorksheetsStore {
    pub db: Db,
}

impl std::fmt::Debug for SlateDBWorksheetsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBWorksheetsStore").finish()
    }
}

impl SlateDBWorksheetsStore {
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    pub async fn new_in_memory() -> Arc<Self> {
        Arc::new(Self::new(Db::memory().await))
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }
}
