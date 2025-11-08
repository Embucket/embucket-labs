use crate::errors;
use crate::errors::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::de;
use slatedb::Db as SlateDb;
use snafu::prelude::*;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::instrument;

#[async_trait]
pub trait ScanIterator: Sized {
    type Collectable;
    async fn collect(mut self) -> Result<Self::Collectable>;
}

#[derive(Clone)]
pub struct VecScanIterator<T: Send + for<'de> serde::de::Deserialize<'de>> {
    db: Option<Arc<SlateDb>>,
    key: String,
    //From where to start the scan range for SlateDB
    // ex: if we ended on "tested2", the cursor would be "tested2"
    // and inside the `fn list_objects` in utils crate the start of the range would be "tested2\x00"
    // ("\x00" is the smallest ASCII char to find anything after the "tested2" excluding it)
    // and the whole range would be `tested2\x00..\x7F
    // (`\x7F` is the largest ASCII char to find anything before it)
    // if there are 4 tables `tested1..tested4` which would yield us "tested3" and "tested4" including other names if any exist
    cursor: Option<String>,
    limit: Option<u16>,
    //Search string, from where (and to where in lexicographical sort order) to do the search
    // ex: if we want to find all the test tables it could be "tes" (if there are 4 tables `tested1..tested4`)
    // the range would be `tes..tes\x7F` tables
    // (`\x7F` is the largest ASCII char to find anything before it)
    // if we however had the cursor from cursor comment (line 21)
    // we could also go from `tested2\x00..tes\x7F` which would yield us "tested3" and "tested4" only excluding other names if any exist
    token: Option<String>,
    // For in-memory mode
    in_memory_items: Option<Vec<T>>,
    marker: PhantomData<T>,
}

impl<T: Send + for<'de> serde::de::Deserialize<'de>> VecScanIterator<T> {
    pub const fn new(db: Arc<SlateDb>, key: String) -> Self {
        Self {
            db: Some(db),
            key,
            cursor: None,
            limit: None,
            token: None,
            in_memory_items: None,
            marker: PhantomData,
        }
    }

    /// Create an in-memory iterator from a vector (used when `SlateDB` is not available)
    #[must_use]
    pub const fn from_vec(items: Vec<T>) -> Self {
        Self {
            db: None,
            key: String::new(),
            cursor: None,
            limit: None,
            token: None,
            in_memory_items: Some(items),
            marker: PhantomData,
        }
    }
    #[must_use]
    pub fn cursor(self, cursor: Option<String>) -> Self {
        Self { cursor, ..self }
    }
    #[must_use]
    pub fn token(self, token: Option<String>) -> Self {
        Self { token, ..self }
    }
    #[must_use]
    pub fn limit(self, limit: Option<u16>) -> Self {
        Self { limit, ..self }
    }
}

#[async_trait]
impl<T: Send + for<'de> serde::de::Deserialize<'de>> ScanIterator for VecScanIterator<T> {
    type Collectable = Vec<T>;

    #[instrument(
        name = "VecScanIterator::collect",
        level = "trace",
        skip(self),
        fields(keys_range, items_count),
        err
    )]
    #[allow(clippy::expect_used)]
    async fn collect(self) -> Result<Self::Collectable> {
        // Handle in-memory mode
        if let Some(items) = self.in_memory_items {
            return Ok(items);
        }

        // SlateDB mode
        let db = self
            .db
            .expect("VecScanIterator requires either db or in_memory_items");

        //We can look with respect to limit
        // from start to end (full scan),
        // from starts_with to start_with (search),
        // from cursor to end (looking not from the start)
        // and from cursor to prefix (search without starting at the start and looking to the end (no full scan))
        // more info in `list_config` file
        let start = self.token.clone().map_or_else(
            || format!("{}/", self.key),
            |search_prefix| format!("{}/{search_prefix}", self.key),
        );
        let start = self
            .cursor
            .map_or_else(|| start, |cursor| format!("{}/{cursor}\x00", self.key));
        let end = self.token.map_or_else(
            || format!("{}/\x7F", self.key),
            |search_prefix| format!("{}/{search_prefix}\x7F", self.key),
        );
        let limit = self.limit.unwrap_or(u16::MAX) as usize;

        // Record the result as part of the current span.
        tracing::Span::current().record("keys_range", format!("{start}..{end}"));

        let range = Bytes::from(start)..Bytes::from(end);
        let mut iter = db.scan(range).await.context(errors::ScanFailedSnafu)?;

        let mut objects = Self::Collectable::new();
        while let Ok(Some(bytes)) = iter.next().await {
            let object = de::from_slice(&bytes.value)
                .context(errors::DeserializeValueSnafu { key: bytes.key })?;
            objects.push(object);
            if objects.len() >= limit {
                break;
            }
        }

        // Record the result as part of the current span.
        tracing::Span::current().record("items_count", objects.len());

        Ok(objects)
    }
}
