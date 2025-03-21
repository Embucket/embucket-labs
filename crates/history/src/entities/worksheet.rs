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

use bytes::Bytes;
use chrono::{DateTime, Utc};
use icebucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub type WorksheetId = i64;

// Worksheet struct is used for storing Query History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Worksheet {
    pub id: WorksheetId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Worksheet {
    #[must_use]
    pub fn get_key(id: WorksheetId) -> Bytes {
        Bytes::from(format!("/ws/{id}"))
    }

    #[must_use]
    pub fn new(content: Option<String>) -> Self {
        let created_at = Utc::now();
        let id = created_at.timestamp_millis();
        // id, start_time have the same value
        Self {
            id,
            name: None,
            content,
            created_at,
            updated_at: created_at,
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    pub fn set_content(&mut self, content: String) {
        self.content = Some(content);
    }
}

impl IterableEntity for Worksheet {
    type Cursor = WorksheetId;

    fn cursor(&self) -> Self::Cursor {
        self.created_at.timestamp_millis()
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.id)
    }
}

#[cfg(test)]
mod test {
    use super::Worksheet;

    #[test]
    fn test_new_worksheet() {
        let w1 = Worksheet::new(None);
        assert_eq!(w1.id, w1.created_at.timestamp_millis());
    }
}
