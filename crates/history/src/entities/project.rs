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

pub type ProjectId = i64;

// Project struct is used for storing Query History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: ProjectId,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl IterableEntity for Project {
    type Cursor = ProjectId;
    const PREFIX: &[u8] = b"pi.";

    fn cursor(&self) -> Self::Cursor {
        self.created_at.timestamp_nanos_opt().unwrap_or(0)
    }
}
