use crate::{DeserializeValueSnafu, Result, ScanFailedSnafu};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::de;
use slatedb::Db as SlateDb;
use snafu::prelude::*;
use std::marker::PhantomData;
use std::sync::Arc;

#[async_trait]
pub trait ScanIterator: Sized {
    type Collectable;
    async fn collect(mut self) -> Result<Self::Collectable>;
}

// #[async_trait]
// pub trait Map: ScanIterator {
//     type Iterator: ScanIterator;
//     type Item: Send + for<'de> serde::de::Deserialize<'de>;
//     type Transformed;
//     type Mapper: FnMut(&Self::Item) -> Self::Transformed;
//     fn map(self, f: Self::Mapper) -> MapScanIterator<Self::Iterator, Self::Item, Self::Transformed, Self::Mapper>;
// }
//
// #[async_trait]
// pub trait Filter: ScanIterator {
//     type Iterator: ScanIterator;
//     type Item: Send + for<'de> serde::de::Deserialize<'de>;
//     type Predicate: FnMut(&Self::Item) -> bool;
//     fn filter(self, f: Self::Predicate) -> FilterScanIterator<Self::Iterator, Self::Item, Self::Predicate>;
// }

#[derive(Clone)]
pub struct VecScanIterator<T: Send + for<'de> serde::de::Deserialize<'de>> {
    db: Arc<SlateDb>,
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
    marker: PhantomData<T>,
}

impl<T: Send + for<'de> serde::de::Deserialize<'de>> VecScanIterator<T> {
    pub const fn new(db: Arc<SlateDb>, key: String) -> Self {
        Self {
            db,
            key,
            cursor: None,
            limit: None,
            token: None,
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

    async fn collect(self) -> Result<Self::Collectable> {
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

        let range = Bytes::from(start)..Bytes::from(end);
        let mut iter = self.db.scan(range).await.context(ScanFailedSnafu)?;

        let mut objects = Self::Collectable::new();
        while let Ok(Some(bytes)) = iter.next().await {
            let object = de::from_slice(&bytes.value).context(DeserializeValueSnafu {
                key: bytes.key,
                data: bytes.value,
            })?;
            objects.push(object);
            if objects.len() >= limit {
                break;
            }
        }
        Ok(objects)
    }
}

// #[async_trait]
// impl<T: Send + for<'de> serde::de::Deserialize<'de>, F: FnMut(&T) -> bool> Filter for VecScanIterator<'_, T> {
//     type Iterator = Self;
//     type Item = Self::Next;
//     type Predicate = F;
//
//     fn filter(self, f: Self::Predicate) -> FilterScanIterator<Self::Iterator, Self::Item, Self::Predicate> {
//         FilterScanIterator {
//             iter: self,
//             filter: f,
//         }
//     }
// }
//
// #[async_trait]
// impl<T: Send + for<'de> serde::de::Deserialize<'de>, U, M: FnMut(&T) -> U> Map for VecScanIterator<'_, T> {
//     type Iterator = Self;
//     type Item = Self::Next;
//     type Transformed = U;
//     type Mapper = M;
//
//     fn map(self, f: Self::Mapper) -> MapScanIterator<Self::Iterator, Self::Item, Self::Transformed, Self::Mapper> {
//         MapScanIterator {
//             iter: self,
//             map: f,
//         }
//     }
// }
//
// pub struct FilterScanIterator<I: ScanIterator, T: Send + for<'de> serde::de::Deserialize<'de>, F: FnMut(&T) -> bool> {
//     iter: I,
//     filter: F,
// }
//
// #[async_trait]
// impl<I: ScanIterator, T: Send + for<'de> serde::de::Deserialize<'de>, F: FnMut(&T) -> bool> ScanIterator for FilterScanIterator<I, T, F>
// {
//     type Next = I::Next;
//
//     async fn next(&mut self) -> Result<Option<Self::Next>> {
//         Ok(self.iter.next().await?.map(async |value| {
//             if self.filter(&value) {
//             Ok(Some(value))
//             } else {
//                 self.next().await
//             }
//         }))
//     }
// }
//
// #[async_trait]
// impl<I: ScanIterator, T: Send + for<'de> serde::de::Deserialize<'de>, F: FnMut(&T) -> bool, U, M: FnMut(&T) -> U> Map for FilterScanIterator<I, T, F> {
//     type Iterator = Self;
//     type Item = Self::Next;
//     type Transformed = U;
//     type Mapper = M;
//
//     fn map(self, f: Self::Mapper) -> MapScanIterator<Self::Iterator, Self::Item, Self::Transformed, Self::Mapper> {
//         MapScanIterator {
//             iter: self,
//             map: f,
//         }
//     }
// }
//
// pub struct MapScanIterator<I: ScanIterator, T: Send + for<'de> serde::de::Deserialize<'de>, U, M: FnMut(&T) -> U> {
//     iter: I,
//     map: M,
// }
//
// #[async_trait]
// impl<I: ScanIterator, T: Send + for<'de> serde::de::Deserialize<'de>, U: std::marker::Send, M: FnMut(&T) -> U> ScanIterator for MapScanIterator<I, T, U, M>
// {
//     type Next = U;
//
//     async fn next(&mut self) -> Result<Option<Self::Next>> {
//         Ok(self.iter.next().await?.map(|value| self.map(&value)))
//     }
// }
//
// #[async_trait]
// impl<I: ScanIterator, T: Send + for<'de> serde::de::Deserialize<'de>, F: FnMut(&T) -> bool, U, M: FnMut(&T) -> U> Filter for MapScanIterator<I, T, U, M> {
//     type Iterator = Self;
//     type Item = Self::Next;
//     type Predicate = F;
//
//     fn filter(self, f: Self::Predicate) -> FilterScanIterator<Self::Iterator, Self::Item, Self::Predicate> {
//         FilterScanIterator {
//             iter: self,
//             filter: f,
//         }
//     }
// }
