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
    // Serve index.html directly instead of redirecting
    match get_file_from_tar("index.html") {
        Err(err) => Err(err),
        Ok(mut content) => {
            // Replace __API_URL__ placeholder in index.html with the actual API URL
            let api_url = std::env::var("API_URL")
                .unwrap_or_else(|_| "http://localhost:3000".to_string());
            let content_str = String::from_utf8_lossy(&content);
            let updated_content = content_str.replace("__API_URL__", &api_url);
            content = updated_content.into_bytes();

            let mime = mime_guess::from_path("index.html")
                .first_raw()
                .unwrap_or("text/html"); // Use text/html as a safe default
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, mime.to_string())
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content))
                .context(ResponseBodySnafu)?)
        }
    }
}

pub async fn tar_handler(Path(path): Path<String>) -> Result<Response> {
    let file_name = path.trim_start_matches(WEB_ASSETS_MOUNT_PATH); // changeable mount path

    // Determine content and effective file name (for MIME type and URL replacement)
    let (mut content, effective_file_name) =
        match get_file_from_tar(file_name) {
            Ok(content) => {
                // Found the requested file (e.g., /styles.css)
                (content, file_name)
            }
            Err(err) => match err {
                Error::WebAssets { source } => match source {
                    WebAssetsError::NotFound { .. } => {
                        // Not found (e.g., /home), serve index.html as fallback for SPA routing
                        match get_file_from_tar("index.html") {
                            Ok(index_content) => (index_content, "index.html"),
                            Err(index_err) => return Err(index_err), // Critical: index.html missing
                        }
                    }
                    err => return Err(err.into()),
                },
                _ => return Err(err),
            },
        };

    // Replace __API_URL__ placeholder in index.html with the actual API URL
    if effective_file_name == "index.html" {
        let api_url = std::env::var("API_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string());
        let content_str = String::from_utf8_lossy(&content);
        let updated_content = content_str.replace("__API_URL__", &api_url);
        content = updated_content.into_bytes();
    }

    // Guess MIME type from the *actual* file being served, not the original path
    let mime = mime_guess::from_path(effective_file_name)
        .first_raw()
        .unwrap_or("application/octet-stream");
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, mime.to_string())
        .header(header::CONTENT_LENGTH, content.len().to_string())
        .body(Body::from(content))
        .context(ResponseBodySnafu)?)
}