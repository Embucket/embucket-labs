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

use axum::response::IntoResponse;
use http::header::InvalidHeaderValue;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum NexusHttpError {
    #[snafu(display("Error parsing Allow-Origin header: {}", source))]
    AllowOriginHeaderParse { source: InvalidHeaderValue },

    #[snafu(display("Session load error: {msg}"))]
    SessionLoad { msg: String },
    #[snafu(display("Unable to persist session"))]
    SessionPersist {
        source: tower_sessions::session::Error,
    },
}

impl IntoResponse for NexusHttpError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::AllowOriginHeaderParse { .. } => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Allow-Origin header parse error",
            )
                .into_response(),
            Self::SessionLoad { .. } => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Session load error",
            )
                .into_response(),
            Self::SessionPersist { .. } => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Session persist error",
            )
                .into_response(),
        }
    }
}
