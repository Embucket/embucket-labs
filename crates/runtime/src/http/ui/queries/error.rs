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
use embucket_history::store::WorksheetsStoreError;
use http::status::StatusCode;
use snafu::prelude::*;

pub type QueriesResult<T> = Result<T, QueriesAPIError>;

pub(crate) type QueryRecordResult<T> = Result<T, QueryError>;

// Query itself can have different kinds of errors
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum QueryError {
    #[snafu(transparent)]
    Execution {
        source: crate::execution::error::ExecutionError,
    },
    #[snafu(transparent)]
    Store { source: WorksheetsStoreError },
    #[snafu(display("Failed to parse row JSON: {source}"))]
    ResultParse { source: serde_json::Error },
    #[snafu(display("ResultSet create error: {source}"))]
    CreateResultSet { source: arrow::error::ArrowError },
    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum QueriesAPIError {
    #[snafu(display("Query execution error: {source}"))]
    Query { source: QueryError },
    #[snafu(display("Error getting queries: {source}"))]
    Queries { source: QueryError },
}

// Select which status code to return.
impl IntoStatusCode for QueriesAPIError {
    #[allow(clippy::match_wildcard_for_single_variants)]
    #[allow(clippy::collapsible_match)]
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Query { source } => match &source {
                QueryError::Execution { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                QueryError::Store { .. } => StatusCode::BAD_REQUEST,
                QueryError::ResultParse { .. }
                | QueryError::Utf8 { .. }
                | QueryError::CreateResultSet { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Queries { source } => match &source {
                QueryError::Store { source } => match &source {
                    WorksheetsStoreError::QueryGet { .. } | WorksheetsStoreError::BadKey { .. } => {
                        StatusCode::NOT_FOUND
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                QueryError::ResultParse { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

// TODO: make it reusable by other *APIError
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
