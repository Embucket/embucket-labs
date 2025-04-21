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

use super::error::{
    ArchiveError, BadArchiveSnafu, HandlerError, NonUnicodeEntryPathInArchiveSnafu,
    ReadEntryDataSnafu, ResponseBodySnafu, Result,
};
use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Redirect, Response},
};
use mime_guess;
use snafu::ResultExt;
use std::io::Cursor;
use std::io::Read;

// Alternative to using tarball is the rust-embed package
const WEB_ASSETS_TARBALL: &[u8] = include_bytes!(env!("WEB_ASSETS_TARBALL_PATH"));
pub const WEB_ASSETS_MOUNT_PATH: &str = "/";

fn get_file_from_tar(file_name: &str) -> Result<Vec<u8>> {
    let cursor = Cursor::new(WEB_ASSETS_TARBALL);

    let mut archive = tar::Archive::new(cursor);

    // It was kind of experiment returning error codes in place
    // TODO: refactor it
    let entries = archive
        .entries()
        .context(BadArchiveSnafu)
        .map_err(|err| HandlerError(StatusCode::INTERNAL_SERVER_ERROR, err))?;
    for entry in entries {
        let mut entry = entry
            .context(BadArchiveSnafu)
            .map_err(|err| HandlerError(StatusCode::INTERNAL_SERVER_ERROR, err))?;
        if entry.header().entry_type() == tar::EntryType::Regular {
            let path = entry
                .path()
                .context(NonUnicodeEntryPathInArchiveSnafu)
                .map_err(|err| HandlerError(StatusCode::UNPROCESSABLE_ENTITY, err))?;
            if path.to_str().unwrap_or_default() == file_name {
                let mut content = Vec::new();
                entry
                    .read_to_end(&mut content)
                    .context(ReadEntryDataSnafu)
                    .map_err(|err| HandlerError(StatusCode::UNPROCESSABLE_ENTITY, err))?;
                return Ok(content);
            }
        }
    }

    // Some requests comes from web app during navigation unintentionally
    // Should we return error if not found?
    // Currently Redirecting instead of returning NOT_FOUND

    Err(HandlerError(
        StatusCode::NOT_FOUND,
        ArchiveError::NotFound {
            path: file_name.to_string(),
        },
    ))
}

pub async fn root_handler() -> Result<Response> {
    Ok(Redirect::to("/index.html").into_response())
}

pub async fn tar_handler(Path(path): Path<String>) -> Result<Response> {
    let file_name = path.trim_start_matches(WEB_ASSETS_MOUNT_PATH); // changeable mount path

    let content = get_file_from_tar(file_name);
    match content {
        Err(err) => {
            if err.0 == StatusCode::NOT_FOUND {
                return Ok(Redirect::to("/index.html").into_response());
            }
            Err(err)
        }
        Ok(content) => {
            let mime = mime_guess::from_path(path)
                .first_raw()
                .unwrap_or("application/octet-stream");
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, mime.to_string())
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content))
                .context(ResponseBodySnafu)
                .map_err(|err| HandlerError(StatusCode::INTERNAL_SERVER_ERROR, err))?)
        }
    }
}
