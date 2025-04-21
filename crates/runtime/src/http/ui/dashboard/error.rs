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

use crate::http::error::ErrorResponse;
use crate::http::ui::error::IntoStatusCode;
use crate::http::ui::queries::error::QueryError;
use axum::response::IntoResponse;
use axum::Json;
use embucket_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

pub type DashboardResult<T> = Result<T, DashboardAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DashboardAPIError {
    #[snafu(display("Get total: {source}"))]
    Metastore { source: MetastoreError },
    #[snafu(display("Get total: {source}"))]
    Queries { source: QueryError },
}

// Select which status code to return.
impl IntoStatusCode for DashboardAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Metastore { .. } | Self::Queries { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// generic
impl IntoResponse for DashboardAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
