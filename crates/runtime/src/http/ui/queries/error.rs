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
use axum::response::IntoResponse;
use axum::Json;
use http::status::StatusCode;
use icebucket_history::store::WorksheetsStoreError;
use snafu::prelude::*;

pub type QueriesResult<T> = Result<T, QueriesAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum QueriesAPIError {
    #[snafu(transparent)]
    QueryExecution {
        source: crate::execution::error::ExecutionError,
    }, // query execution error
    #[snafu(display("Query worksheet error: {source}"))]
    QueryWorksheet { source: WorksheetsStoreError }, // query worksheet error
    #[snafu(display("Error getting queries: {source}"))]
    Queries { source: WorksheetsStoreError },
}

// Select which status code to return.
impl IntoStatusCode for QueriesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            // query handler error
            Self::QueryExecution { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            // query handler error
            Self::QueryWorksheet { source } => match &source {
                WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::BAD_REQUEST,
            },
            Self::Queries { source } => match &source {
                WorksheetsStoreError::HistoryGet { .. }
                | WorksheetsStoreError::WorksheetNotFound { .. }
                | WorksheetsStoreError::BadKey { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

// generic
impl IntoResponse for QueriesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
