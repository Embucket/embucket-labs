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

use axum::{response::IntoResponse, response::Response};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RuntimeHttpError {
    #[snafu(transparent)]
    Metastore {
        source: crate::http::metastore::error::MetastoreAPIError,
    },
    #[snafu(transparent)]
    Dbt {
        source: crate::http::dbt::error::DbtError,
    },
    #[snafu(transparent)]
    UI {
        source: crate::http::ui::error::UIError,
    },
}

impl IntoResponse for RuntimeHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::Metastore { source } => source.into_response(),
            Self::Dbt { source } => source.into_response(),
            Self::UI { source } => source.into_response(),
        }
    }
}

//pub struct RuntimeHttpResult<T>(pub T);
pub type RuntimeHttpResult<T> = Result<T, RuntimeHttpError>;

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorResponse(\"{}\")", self.message)
    }
}
