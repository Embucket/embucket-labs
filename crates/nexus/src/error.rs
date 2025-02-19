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

use axum::http::StatusCode;
use axum::{response::IntoResponse, response::Response};
use catalog::error::CatalogError;
use control_plane::error::ControlPlaneError;

impl From<ControlPlaneError> for AppError {
    fn from(err: ControlPlaneError) -> Self {
        Self {
            message: err.to_string(),
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Map according to spec <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
impl From<CatalogError> for AppError {
    fn from(err: CatalogError) -> Self {
        let status = match err {
            CatalogError::DatabaseNotFound { .. } | CatalogError::TableNotFound { .. } => {
                StatusCode::NOT_FOUND
            }
            CatalogError::NamespaceAlreadyExists { .. }
            | CatalogError::TableAlreadyExists { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            message: err.to_string(),
            status_code: status,
        }
    }
}

#[derive(Debug)]
pub struct AppError {
    pub message: String,
    pub status_code: StatusCode,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let message = (if self.message.is_empty() {
            "Internal Server Error".to_string()
        } else {
            self.message
        },);
        (self.status_code, message).into_response()
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppError(\"{}\")", self.message)
    }
}
