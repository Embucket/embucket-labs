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

use chrono::NaiveDateTime;
use icebucket_metastore::models::{IceBucketSchema, IceBucketSchemaIdent};
use icebucket_metastore::RwObject;
use serde::{Deserialize, Serialize};
use std::convert::From;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Schema {
    pub name: String,
    pub database: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl From<RwObject<IceBucketSchema>> for Schema {
    fn from(rw_schema: RwObject<IceBucketSchema>) -> Self {
        Self {
            name: rw_schema.data.ident.schema,
            database: rw_schema.data.ident.database,
            created_at: rw_schema.created_at,
            updated_at: rw_schema.updated_at,
        }
    }
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
#[allow(clippy::from_over_into)]
impl Into<IceBucketSchema> for Schema {
    fn into(self) -> IceBucketSchema {
        IceBucketSchema {
            ident: IceBucketSchemaIdent {
                schema: self.name,
                database: self.database,
            },
            properties: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCreatePayload {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaUpdatePayload {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaUpdateResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCreateResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemasResponse {
    pub items: Vec<Schema>,
    pub current_cursor: Option<String>,
    pub next_cursor: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct SchemasParameters {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub search: Option<String>,
}
