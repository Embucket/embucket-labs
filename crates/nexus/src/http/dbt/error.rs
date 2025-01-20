use axum::{http, response::IntoResponse, Json};
use snafu::prelude::*;

use super::schemas::JsonResponse;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum DbtError {
    #[snafu(display("Failed to decompress GZip body"))]
    GZipDecompress { source: std::io::Error },

    #[snafu(display("Failed to parse login request"))]
    LoginRequestParse { source: serde_json::Error },

    #[snafu(display("Failed to parse query body"))]
    QueryBodyParse { source: serde_json::Error },

    #[snafu(display("Internal error"))]
    ControlService {
        source: control_plane::error::ControlPlaneError,
    },

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
}

pub type DbtResult<T> = std::result::Result<T, DbtError>;

impl IntoResponse for DbtError {
    fn into_response(self) -> axum::response::Response<axum::body::Body> {
        let status_code = match &self {
            Self::GZipDecompress { .. }
            | Self::LoginRequestParse { .. }
            | Self::QueryBodyParse { .. }
            | Self::InvalidWarehouseIdFormat { .. } => http::StatusCode::BAD_REQUEST,
            Self::ControlService { .. } | Self::RowParse { .. } => {
                http::StatusCode::INTERNAL_SERVER_ERROR
            }
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
            Self::ControlService { source } => source.to_string(),
            Self::RowParse { source } => format!("failed to parse row JSON: {source}"),
            Self::MissingAuthToken | Self::MissingDbtSession | Self::InvalidAuthData => {
                "session error".to_string()
            }
            Self::NotImplemented => "feature not implemented".to_string(),
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
