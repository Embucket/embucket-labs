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

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub struct CreateTablePayload {
//     pub(crate) name: String,
//     pub(crate) columns: Vec<TableColumn>,
// }
//

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableResponse {
    pub(crate) name: String,
    pub(crate) columns: Vec<TableColumn>,
    pub(crate) total_rows: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableColumn {
    pub(crate) name: String,
    pub(crate) r#type: String,
}
