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
use axum::response::IntoResponse;
use axum::Json;
use http::status::StatusCode;
use icebucket_history::store::WorksheetsStoreError;
use snafu::prelude::*;

pub type WorksheetsResult<T> = Result<T, WorksheetsAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum WorksheetsAPIError {
    #[snafu(display("Create worksheet error: {source}"))]
    Create { source: WorksheetsStoreError },
    #[snafu(display("Get worksheet error: {source}"))]
    Get { source: WorksheetsStoreError },
    #[snafu(display("Delete worksheet error: {source}"))]
    Delete { source: WorksheetsStoreError },
    #[snafu(display("Update worksheet error: {source}"))]
    Update { source: WorksheetsStoreError },
    #[snafu(display("Get worksheets error: {source}"))]
    List { source: WorksheetsStoreError },
}

trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

// Select which status code to return.
impl IntoStatusCode for WorksheetsAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                WorksheetsStoreError::WorksheetAdd { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } => match &source {
                WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                WorksheetsStoreError::BadKey { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source } => match &source {
                WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &source {
                WorksheetsStoreError::BadKey { .. }
                | WorksheetsStoreError::WorksheetUpdate { .. } => StatusCode::BAD_REQUEST,
                WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { source } => match &source {
                WorksheetsStoreError::WorksheetsList { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

// generic
impl IntoResponse for WorksheetsAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
