use super::error::{
    ArchiveError, BadArchiveSnafu, HandlerError, NonUnicodeEntryPathInArchiveSnafu,
    ReadEntryDataSnafu, ResponseBodySnafu, Result,
};
use api_ui_static_assets::WEB_ASSETS_TARBALL;
use axum::{
    body::Body,
    extract::Path,
    http::{StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use mime_guess;
use snafu::ResultExt;
use std::io::Cursor;
use std::io::Read;

// Alternative to using tarball is the rust-embed package
pub const WEB_ASSETS_MOUNT_PATH: &str = "/";

fn get_file_from_tar(file_name: &str) -> Result<Vec<u8>> {
    let file_name = "dist/".to_owned() + file_name;
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
        ArchiveError::NotFound { path: file_name },
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
