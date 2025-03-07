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

use std::ops::Deref;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

pub mod database;
pub mod schema;
pub mod table;
pub mod volumes;

pub use database::*;
pub use schema::*;
pub use table::*;
pub use volumes::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct RwObject<T>
where
    T: Eq + PartialEq,
{
    #[serde(flatten)]
    pub data: T,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl<T> RwObject<T>
where
    T: Eq + PartialEq,
{
    pub fn new(data: T) -> Self {
        let now = chrono::Utc::now().naive_utc();
        Self {
            data,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn update(&mut self, data: T) {
        if data != self.data {
            self.data = data;
            self.updated_at = chrono::Utc::now().naive_utc();
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now().naive_utc();
    }
}

impl<T> Deref for RwObject<T>
where
    T: Eq + PartialEq,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
