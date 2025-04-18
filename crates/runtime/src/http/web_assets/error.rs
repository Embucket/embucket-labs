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
use http::Error;
use http::StatusCode;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ArchiveError {
    #[snafu(display("File not found: {path}"))]
    NotFound { path: String },

    #[snafu(display("Response body error: {source}"))]
    ResponseBody { source: Error },

    #[snafu(display("Bad archive: {source}"))]
    BadArchive { source: std::io::Error },

    #[snafu(display("Entry path is not a valid Unicode: {source}"))]
    NonUnicodeEntryPathInArchive { source: std::io::Error },

    #[snafu(display("Entry data read error: {source}"))]
    ReadEntryData { source: std::io::Error },
}

pub struct HandlerError(pub StatusCode, pub ArchiveError);

pub type Result<T> = std::result::Result<T, HandlerError>;

impl IntoResponse for HandlerError {
    fn into_response(self) -> axum::response::Response {
        let code = self.0;
        let error = self.1;
        let error = ErrorResponse {
            message: error.to_string(),
            status_code: code.as_u16(),
        };
        (code, Json(error)).into_response()
    }
}
