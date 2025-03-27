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

use crate::http::metastore::error::MetastoreAPIError;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum UIError {
    #[snafu(transparent)]
    Execution {
        source: crate::execution::error::ExecutionError,
    },
    #[snafu(transparent)]
    Metastore {
        source: icebucket_metastore::error::MetastoreError,
    },
}
pub type UIResult<T> = Result<T, UIError>;

pub(crate) trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub(crate) struct UIResponse<T> {
//     #[serde(flatten)]
//     pub(crate) data: T,
// }
//
// impl<T> UIResponse<T> {
//     pub const fn from(data: T) -> Json<Self> {
//         Json(Self { data })
//     }
// }

impl IntoResponse for UIError {
    fn into_response(self) -> Response<axum::body::Body> {
        match self {
            Self::Execution { source } => source.into_response(),
            Self::Metastore { source } => MetastoreAPIError(source).into_response(),
        }
    }
}
