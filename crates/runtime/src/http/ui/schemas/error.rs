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

use crate::execution::error::ExecutionError;
use crate::http::error::ErrorResponse;
use crate::http::ui::error::IntoStatusCode;
use axum::response::IntoResponse;
use axum::Json;
use embucket_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;
use crate::execution::error::ExecutionError;

pub type SchemasResult<T> = Result<T, SchemasAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SchemasAPIError {
    #[snafu(display("Create schema error: {source}"))]
    Create { source: ExecutionError },
    #[snafu(display("Get schema error: {source}"))]
    Get { source: MetastoreError },
    #[snafu(display("Delete schema error: {source}"))]
    Delete { source: ExecutionError },
    #[snafu(display("Update schema error: {source}"))]
    Update { source: MetastoreError },
    #[snafu(display("Get schemas error: {source}"))]
    List { source: MetastoreError },
    #[snafu(display("Query engine error: {source}"))]
    Query { source: ExecutionError },
}

// Select which status code to return.
impl IntoStatusCode for SchemasAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                ExecutionError::Metastore { source } => match source {
                    MetastoreError::SchemaAlreadyExists { .. }
                    | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                    MetastoreError::DatabaseNotFound { .. } | MetastoreError::Validation { .. } => {
                        StatusCode::BAD_REQUEST
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } => match &source {
                MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Delete { source } => match &source {
                ExecutionError::Metastore {
                    source: MetastoreError::SchemaNotFound { .. },
                } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &source {
                MetastoreError::SchemaNotFound { .. } => StatusCode::NOT_FOUND,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Query { .. } => StatusCode::INTERNAL_SERVER_ERROR,       
        }
    }
}

// TODO: make it reusable by other *APIError
impl IntoResponse for SchemasAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
