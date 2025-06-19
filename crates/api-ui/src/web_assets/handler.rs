use super::error::{
    self as error, BadArchiveSnafu, Error as WebAssetsError, NonUnicodeEntryPathInArchiveSnafu,
    ReadEntryDataSnafu, ResponseBodySnafu,
};
use crate::error::{Error, Result};
use api_ui_static_assets::WEB_ASSETS_TARBALL;
use axum::{
    body::Body,
    extract::Path,
    http::header,
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

    let entries = archive.entries().context(BadArchiveSnafu)?;
    for entry in entries {
        let mut entry = entry.context(BadArchiveSnafu)?;
        if entry.header().entry_type() == tar::EntryType::Regular {
            let path = entry.path().context(NonUnicodeEntryPathInArchiveSnafu)?;
            if path.to_str().unwrap_or_default() == file_name {
                let mut content = Vec::new();
                entry
                    .read_to_end(&mut content)
                    .context(ReadEntryDataSnafu)?;
                return Ok(content);
            }
        }
    }

    Err(error::NotFoundSnafu { path: file_name }.build().into())
}

pub async fn root_handler() -> Result<Response> {
    Ok(Redirect::to("/index.html").into_response())
}

pub async fn tar_handler(Path(path): Path<String>) -> Result<Response> {
    let file_name = path.trim_start_matches(WEB_ASSETS_MOUNT_PATH); // changeable mount path

    let content = get_file_from_tar(file_name);
    match content {
        Err(err) => match err {
            Error::WebAssets { source } => match source {
                WebAssetsError::NotFound { .. } => Ok(Redirect::to("/index.html").into_response()),
                err => Err(err.into()),
            },
            _ => Err(err),
        },
        Ok(content) => {
            let mime = mime_guess::from_path(path)
                .first_raw()
                .unwrap_or("application/octet-stream");
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, mime.to_string())
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content))
                .context(ResponseBodySnafu)?)
        }
    }
}
