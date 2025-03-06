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

use crate::HistoryItem;
use async_trait::async_trait;
use icebucket_utils::IterableEntity;
use snafu::prelude::*;


#[derive(Snafu, Debug)]
pub enum QHistoryError {
    #[snafu(display("Error adding query history: {source}"))]
    QHistoryCreate { source: icebucket_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    QHistoryGet { source: icebucket_utils::Error },
}

pub type QHistoryResult<T> = std::result::Result<T, QHistoryError>;


#[async_trait]
pub trait QHistoryApi: Send + Sync {
    async fn create_history_item(&self, item: HistoryItem) -> QHistoryResult<()>;
    async fn query_history(&self, cursor: &[u8], limit: u16) -> QHistoryResult<Vec<HistoryItem>>;
}

pub struct QHistoryStore {
    db: icebucket_utils::Db,
}

#[async_trait]
impl QHistoryApi for QHistoryStore {
    async fn create_history_item(&self, item: HistoryItem) -> QHistoryResult<()> {
        Ok(self.db.put_iterable_entity(&item)
            .await
            .context(QHistoryCreateSnafu)?)
    }

    async fn query_history(&self, cursor: &[u8], limit: u16) -> QHistoryResult<Vec<HistoryItem>> {
        let start_key = HistoryItem::key_with_prefix_u8(cursor);
        Ok(self.db.items_from_range(
            start_key..HistoryItem::max_key(), Some(limit),
        ).await.context(QHistoryGetSnafu)?)
    }
}
