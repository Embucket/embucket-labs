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

use axum::{http, response::IntoResponse, Json};
use snafu::prelude::*;

use super::schemas::JsonResponse;
use arrow::error::ArrowError;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum DbtError {
    #[snafu(display("Failed to decompress GZip body"))]
    GZipDecompress { source: std::io::Error },

    #[snafu(display("Failed to parse login request"))]
    LoginRequestParse { source: serde_json::Error },

    #[snafu(display("Failed to parse query body"))]
    QueryBodyParse { source: serde_json::Error },

    #[snafu(display("Missing auth token"))]
    MissingAuthToken,

    #[snafu(display("Invalid warehouse_id format"))]
    InvalidWarehouseIdFormat { source: uuid::Error },

    #[snafu(display("Missing DBT session"))]
    MissingDbtSession,

    #[snafu(display("Invalid auth data"))]
    InvalidAuthData,

    #[snafu(display("Feature not implemented"))]
    NotImplemented,

    #[snafu(display("Failed to parse row JSON"))]
    RowParse { source: serde_json::Error },

    #[snafu(display("UTF8 error: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(transparent)]
    Metastore {
        source: crate::http::metastore::error::MetastoreAPIError,
    },

    #[snafu(transparent)]
    Execution {
        source: crate::execution::error::ExecutionError,
    },
}

pub type DbtResult<T> = std::result::Result<T, DbtError>;

impl IntoResponse for DbtError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        if let Self::Execution { source } = self {
            return source.into_response();
        }
        if let Self::Metastore { source } = self {
            return source.into_response();
        }

        let status_code = match &self {
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidWarehouseIdFormat { .. } => http::StatusCode::BAD_REQUEST,
            Self::RowParse { .. }
            | Self::Utf8 { .. }
            | Self::Arrow { .. }
            | Self::Metastore { .. }
            | Self::Execution { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::MissingAuthToken | Self::MissingDbtSession | Self::InvalidAuthData => {
                http::StatusCode::UNAUTHORIZED
            }
            Self::NotImplemented => http::StatusCode::NOT_IMPLEMENTED,
        };

        let message = match &self {
            Self::GZipDecompress { source } => format!("failed to decompress GZip body: {source}"),
            Self::LoginRequestParse { source } => {
                format!("failed to parse login request: {source}")
            }
            Self::QueryBodyParse { source } => format!("failed to parse query body: {source}"),
            Self::InvalidWarehouseIdFormat { source } => format!("invalid warehouse_id: {source}"),
            Self::RowParse { source } => format!("failed to parse row JSON: {source}"),
            Self::MissingAuthToken | Self::MissingDbtSession | Self::InvalidAuthData => {
                "session error".to_string()
            }
            Self::Utf8 { source } => {
                format!("Error encoding UTF8 string: {source}")
            }
            Self::Arrow { source } => {
                format!("Error encoding in Arrow format: {source}")
            }
            Self::NotImplemented => "feature not implemented".to_string(),
            Self::Metastore { source } => source.to_string(),
            Self::Execution { source } => source.to_string(),
        };

        let body = Json(JsonResponse {
            success: false,
            message: Some(message),
            data: None,
            code: Some(status_code.as_u16().to_string()),
        });
        (status_code, body).into_response()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_in_result)]
mod tests {

    // TODO: Replace these with snapshot tests
    /*#[test]
        fn test_http_server_response() {
            assert_ne!(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                DbtError::ControlService {
                    source: ControlPlaneError::Execution {
                        source: IceBucketSQLError::Arrow {
                            source: ArrowError::ComputeError(String::new())
                        }
                    },
                }
                .into_response()
                .status(),
            );
            assert_eq!(
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                DbtError::ControlService {
                    source: ControlPlaneError::Execution {
                        source: IceBucketSQLError::Arrow {
                            source: ArrowError::ComputeError(String::new())
                        }
                    },
                }
                .into_response()
                .status(),
            );
            assert_eq!(
                http::StatusCode::UNPROCESSABLE_ENTITY,
                DbtError::ControlService {
                    source: ControlPlaneError::Execution {
                        source: IceBucketSQLError::DataFusion {
                            source: DataFusionError::ArrowError(
                                ArrowError::InvalidArgumentError(String::new()),
                                Some(String::new()),
                            )
                        },
                    },
                }
                .into_response()
                .status(),
            );
            assert_eq!(
                http::StatusCode::NOT_FOUND,
                DbtError::ControlService {
                    source: ControlPlaneError::WarehouseNameNotFound {
                        name: String::new()
                    },
                }
                .into_response()
                .status(),
            );
            assert_eq!(
                http::StatusCode::NOT_FOUND,
                DbtError::ControlService {
                    source: ControlPlaneError::WarehouseNotFound { id: Uuid::new_v4() },
                }
                .into_response()
                .status(),
            );
            assert_eq!(
                http::StatusCode::NOT_FOUND,
                DbtError::ControlService {
                    source: ControlPlaneError::WarehouseNotFound { id: Uuid::new_v4() },
                }
                .into_response()
                .status(),
            );
            assert_eq!(
                http::StatusCode::UNPROCESSABLE_ENTITY,
                DbtError::ControlService {
                    source: ControlPlaneError::DataFusion {
                        // here just any error for test, since we are handling any DataFusion err
                        source: DataFusionError::ArrowError(
                            ArrowError::InvalidArgumentError(String::new()),
                            Some(String::new()),
                        )
                    }
                }
                .into_response()
                .status(),
            );
        }
    */
}
