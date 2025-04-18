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

use crate::{QueryRecordId, WorksheetId};
use bytes::Bytes;
use embucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// QueryRecordReference struct is used for referencing QueryRecord from worksheet
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct QueryRecordReference {
    #[serde(skip_serializing)]
    pub id: QueryRecordId,
    #[serde(skip_serializing)]
    pub worksheet_id: WorksheetId,
}

impl QueryRecordReference {
    #[must_use]
    pub fn get_key(worksheet_id: WorksheetId, id: QueryRecordId) -> Bytes {
        Bytes::from(format!("/ws/{worksheet_id}/qh/{id}"))
    }

    pub fn extract_qh_key(data: &Bytes) -> Option<Bytes> {
        let pattern = b"/qh/";
        data.windows(pattern.len())
            .position(|w| w == pattern)
            .map(|pos| data.slice(pos..))
    }
}

impl IterableEntity for QueryRecordReference {
    type Cursor = QueryRecordId;

    fn cursor(&self) -> Self::Cursor {
        self.id
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.worksheet_id, self.id)
    }
}
