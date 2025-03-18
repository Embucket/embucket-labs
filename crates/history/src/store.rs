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

use crate::{Project, ProjectId, QueryHistoryItem};
use async_trait::async_trait;
use bytes::Bytes;
use icebucket_utils::iterable::{IterableCursor, IterableEntity};
use icebucket_utils::Db;
use snafu::prelude::*;
// use serde::{Serialize, Deserialize};
// use serde_json;
#[cfg(test)]
use std::sync::Arc;

#[derive(Snafu, Debug)]
pub enum ProjectsStoreError {
    #[snafu(display("Error using key: {source}"))]
    BadKey { source: std::str::Utf8Error },

    #[snafu(display("Error adding project: {source}"))]
    ProjectAdd { source: icebucket_utils::Error },

    #[snafu(display("Error getting project: {source}"))]
    ProjectGet { source: icebucket_utils::Error },

    #[snafu(display("Error deleting project: {source}"))]
    ProjectDelete { source: icebucket_utils::Error },

    #[snafu(display("Error adding query history: {source}"))]
    HistoryAdd { source: icebucket_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    HistoryGet { source: icebucket_utils::Error },
    // #[snafu(display("Error deserialising value: {source}"))]
    // Deserialize { source:: serde_json::error::Error },
}

pub type ProjectsStoreResult<T> = std::result::Result<T, ProjectsStoreError>;

#[async_trait]
pub trait ProjectsStore: std::fmt::Debug + Send + Sync {
    async fn add_project(&self, project: Project) -> ProjectsStoreResult<ProjectId>;
    async fn get_project(&self, id: ProjectId) -> ProjectsStoreResult<Option<Project>>;
    // async fn update_project(&self, id: ProjectId) -> ProjectsStoreResult<Project>;
    async fn delete_project(&self, id: ProjectId) -> ProjectsStoreResult<()>;
    async fn get_projects(&self) -> ProjectsStoreResult<Vec<Project>>;

    async fn add_history_item(&self, item: QueryHistoryItem) -> ProjectsStoreResult<()>;
    async fn query_history(
        &self,
        cursor: Option<String>,
        limit: Option<u16>,
    ) -> ProjectsStoreResult<Vec<QueryHistoryItem>>;
}

pub struct SlateDBProjectsStore {
    db: Db,
}

impl std::fmt::Debug for SlateDBProjectsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBProjectsStore").finish()
    }
}

impl SlateDBProjectsStore {
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    #[cfg(test)]
    pub async fn new_in_memory() -> Arc<Self> {
        Arc::new(Self::new(Db::memory().await))
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }

    // async fn patch_object<T, C>(&self, id: C, patch: String) -> ProjectsStoreResult<T>
    //     where
    //         T: for<'de> serde::de::Deserialize<'de> + Serialize + IterableEntity,
    //         C: IterableCursor,
    // {
    //     // get object
    //     // convert from Bytes to &str, for .get method to convert it back to Bytes
    //     let key_bytes= Project::key_from_cursor(id.as_bytes());
    //     let key_str = std::str::from_utf8(key_bytes.as_ref())
    //         .context(BadKeySnafu)?;

    //     let object: Option<T> = self.db.get(key_str).await.context(ProjectGetSnafu)?;

    //     // serialize to json should not fail
    //     let mut json = serde_json::to_string(&project).unwrap();
    //     // patch using json patch
    //     patch(&mut json, &p).unwrap();
    //     // deserialize to object
    //     serde_json::from_str(s)
    //     // save back
    // }
}

#[async_trait]
impl ProjectsStore for SlateDBProjectsStore {
    async fn add_project(&self, project: Project) -> ProjectsStoreResult<ProjectId> {
        self.db
            .put_iterable_entity(&project)
            .await
            .context(ProjectAddSnafu)?;
        Ok(project.id)
    }

    async fn get_project(&self, id: ProjectId) -> ProjectsStoreResult<Option<Project>> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Project::key_from_cursor(id.as_bytes());
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(BadKeySnafu)?;

        Ok(self.db.get(key_str).await.context(ProjectGetSnafu)?)
    }

    // async fn update_project(&self, id: ProjectId) -> ProjectsStoreResult<Project> {
    //     self.patch_object(id)

    //     // get object
    //     let project_by_id = self.get_project(id).await?;
    //     // serialize to json should not fail
    //     let mut prjson = serde_json::to_string(&project_by_id).unwrap();
    //     // patch using json patch
    //     patch(&mut doc, &p).unwrap();
    //     // deserialize to object
    //     // save back
    // }

    async fn delete_project(&self, id: ProjectId) -> ProjectsStoreResult<()> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Project::key_from_cursor(id.as_bytes());
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(BadKeySnafu)?;

        Ok(self.db.delete(key_str).await.context(ProjectDeleteSnafu)?)
    }

    async fn get_projects(&self) -> ProjectsStoreResult<Vec<Project>> {
        let start_key = Project::min_key();
        let end_key = Project::max_key();
        Ok(self
            .db
            .items_from_range(start_key..end_key, None)
            .await
            .context(HistoryGetSnafu)?)
    }

    async fn add_history_item(&self, item: QueryHistoryItem) -> ProjectsStoreResult<()> {
        Ok(self
            .db
            .put_iterable_entity(&item)
            .await
            .context(HistoryAddSnafu)?)
    }

    async fn query_history(
        &self,
        cursor: Option<String>,
        limit: Option<u16>,
    ) -> ProjectsStoreResult<Vec<QueryHistoryItem>> {
        let start_key = if let Some(cursor) = cursor {
            QueryHistoryItem::key_from_cursor(Bytes::from(cursor))
        } else {
            QueryHistoryItem::min_key()
        };
        let end_key = QueryHistoryItem::max_key();
        Ok(self
            .db
            .items_from_range(start_key..end_key, limit)
            .await
            .context(HistoryGetSnafu)?)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use icebucket_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;

    #[tokio::test]
    async fn test_history() {
        let db = SlateDBProjectsStore::new_in_memory().await;
        let n: u16 = 2;
        let mut created: Vec<QueryHistoryItem> = vec![];
        for i in 0..n {
            let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                + Duration::milliseconds(i.into());
            let mut item = QueryHistoryItem::query_start(
                format!("select {i}").as_str(),
                None,
                Some(start_time),
            );
            if i == 0 {
                item.query_finished(1, Some(item.start_time))
            } else {
                item.query_finished_with_error("Test query pseudo error".to_string());
            }
            created.push(item.clone());
            println!("added {:?}", item.key());
            db.add_history_item(item).await.unwrap();
        }

        let cursor = <QueryHistoryItem as IterableEntity>::Cursor::CURSOR_MIN.to_string();
        println!("cursor: {cursor}");
        let retrieved = db.query_history(Some(cursor), Some(10)).await.unwrap();
        for i in 0..retrieved.len() {
            println!("retrieved: {:?}", retrieved[i].key());
        }

        assert_eq!(n as usize, retrieved.len());
        assert_eq!(created, retrieved);
    }
}
