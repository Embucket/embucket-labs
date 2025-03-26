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

use icebucket_metastore::models::IceBucketDatabase;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Database {
    pub database: String,
    pub volume: String,
}

impl From<IceBucketDatabase> for Database {
    fn from(db: IceBucketDatabase) -> Self {
        Database {
            database: db.ident,
            volume: db.volume,
        }
    }
}

// do not actualy use Database instead of IceBucketDatabase as
// it's just enough updating value_type in utoipa for non-nested struct

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabasePayload {
    #[serde(flatten)]
    #[schema(value_type = Database)]
    pub data: IceBucketDatabase,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseResponse {
    #[serde(flatten)]
    #[schema(value_type = Database)]
    pub data: IceBucketDatabase,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabasesResponse {
    #[schema(value_type = Vec<Database>)]
    pub items: Vec<IceBucketDatabase>,
}
