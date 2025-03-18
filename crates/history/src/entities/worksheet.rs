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
    pub content: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Worksheet {
    #[must_use]
    pub fn new(id: Option<WorksheetId>, content: Option<String>) -> Self {
        let id = id.unwrap_or_else(|| Utc::now().timestamp_millis());
        let created_at = DateTime::<Utc>::from_timestamp_nanos(id);
        // id, start_time have the same value
        Self {
            id: created_at.timestamp_millis(),
            content,
            created_at,
            updated_at: created_at,
        }
    }
}

impl IterableEntity for Worksheet {
    type Cursor = WorksheetId;
    const PREFIX: &[u8] = b"pi.";

    fn cursor(&self) -> Self::Cursor {
        self.created_at.timestamp_nanos_opt().unwrap_or(0)
    }
}

#[cfg(test)]
mod test {
    use super::Worksheet;

    #[test]
    fn test_new_worksheet() {
        let w1 = Worksheet::new(None, None);
        assert_eq!(w1.id, w1.created_at.timestamp_millis());

        let w1 = Worksheet::new(Some(436324634634), None);
        assert_eq!(w1.id, w1.created_at.timestamp_millis());
    }
}
