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
use http::StatusCode;
use icebucket_metastore::error::MetastoreError;
use snafu::prelude::*;
pub type VolumesResult<T> = Result<T, VolumesAPIError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum VolumesAPIError {
    #[snafu(display("Create volumes error: {source}"))]
    Create { source: MetastoreError },
    #[snafu(display("Get volume error: {source}"))]
    Get { source: MetastoreError },
    #[snafu(display("Delete volume error: {source}"))]
    Delete { source: MetastoreError },
    #[snafu(display("Update volume error: {source}"))]
    Update { source: MetastoreError },
    #[snafu(display("Get volume error: {source}"))]
    List { source: MetastoreError },
}

// Select which status code to return.
impl IntoStatusCode for VolumesAPIError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source } => match &source {
                MetastoreError::VolumeAlreadyExists { .. }
                | MetastoreError::ObjectAlreadyExists { .. } => StatusCode::CONFLICT,
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Get { source } | Self::Delete { source } => match &source {
                MetastoreError::UtilSlateDB { .. } | MetastoreError::ObjectNotFound { .. } => {
                    StatusCode::NOT_FOUND
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Update { source } => match &source {
                MetastoreError::ObjectNotFound { .. } | MetastoreError::VolumeNotFound { .. } => {
                    StatusCode::NOT_FOUND
                }
                MetastoreError::Validation { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::List { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// generic
impl IntoResponse for VolumesAPIError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();
        let error = ErrorResponse {
            message: self.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
