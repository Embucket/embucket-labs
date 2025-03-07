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
use axum::{response::IntoResponse, Json};
use icebucket_metastore::error::MetastoreError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub struct MetastoreAPIError(pub MetastoreError);
pub type MetastoreAPIResult<T> = Result<T, MetastoreAPIError>;

impl IntoResponse for MetastoreAPIError {
    fn into_response(self) -> axum::response::Response {
        let message = (self.0.to_string(),);
        let code = match self.0 {
            MetastoreError::TableDataExists { .. } => http::StatusCode::CONFLICT,
            MetastoreError::TableRequirementFailed { .. } => http::StatusCode::UNPROCESSABLE_ENTITY,
            MetastoreError::VolumeValidationFailed { .. } |
            MetastoreError::VolumeMissingCredentials => http::StatusCode::BAD_REQUEST,
            MetastoreError::CloudProviderNotImplemented { .. } => {
                http::StatusCode::PRECONDITION_FAILED
            }
            MetastoreError::ObjectStore { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::CreateDirectory { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::SlateDB { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::UtilSlateDB { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::ObjectAlreadyExists { .. } => http::StatusCode::CONFLICT,
            MetastoreError::ObjectNotFound { .. } => http::StatusCode::NOT_FOUND,
            MetastoreError::VolumeInUse { .. } => http::StatusCode::CONFLICT,
            MetastoreError::Iceberg { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::Serde { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::Validation { .. } => http::StatusCode::BAD_REQUEST,
            MetastoreError::TableMetadataBuilder { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
        };

        let error = ErrorResponse {
            message: message.0,
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
