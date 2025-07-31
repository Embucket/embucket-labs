use axum::Json;
use axum::response::IntoResponse;
use core_executor::SnowflakeError;
use error_stack::ErrorExt;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::fmt::Debug;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(transparent)]
    Auth { source: crate::auth::Error },

    #[snafu(transparent)]
    Dashboard {
        #[snafu(source(from(crate::dashboard::Error, Box::new)))]
        source: Box<crate::dashboard::Error>,
    },

    #[snafu(transparent)]
    Databases {
        #[snafu(source(from(crate::databases::Error, Box::new)))]
        source: Box<crate::databases::Error>,
    },

    #[snafu(transparent)]
    NavigationTrees {
        source: crate::navigation_trees::Error,
    },

    #[snafu(transparent)]
    QueriesError {
        #[snafu(source(from(crate::queries::Error, Box::new)))]
        source: Box<crate::queries::Error>,
    },

    #[snafu(transparent)]
    Schemas {
        #[snafu(source(from(crate::schemas::Error, Box::new)))]
        source: Box<crate::schemas::Error>,
    },

    #[snafu(transparent)]
    Tables {
        #[snafu(source(from(crate::tables::Error, Box::new)))]
        source: Box<crate::tables::Error>,
    },

    #[snafu(transparent)]
    Volumes {
        #[snafu(source(from(crate::volumes::Error, Box::new)))]
        source: Box<crate::volumes::Error>,
    },

    #[snafu(transparent)]
    WebAssets { source: crate::web_assets::Error },

    #[snafu(transparent)]
    Worksheets {
        #[snafu(source(from(crate::worksheets::Error, Box::new)))]
        source: Box<crate::worksheets::Error>,
    },
}

pub(crate) trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub message: String,
    pub status_code: u16,
}

impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Auth { source } => source.status_code(),
            Self::Dashboard { source } => source.status_code(),
            Self::Databases { source } => source.status_code(),
            Self::NavigationTrees { source } => source.status_code(),
            Self::QueriesError { source } => source.status_code(),
            Self::Schemas { source } => source.status_code(),
            Self::Tables { source } => source.status_code(),
            Self::Volumes { source } => source.status_code(),
            Self::WebAssets { source } => source.status_code(),
            Self::Worksheets { source } => source.status_code(),
        }
    }
}

impl IntoResponse for Error {
    #[tracing::instrument(
        name = "api-ui::Error::into_response",
        level = "info",
        fields(display_error, debug_error, error_stack_trace, status_code),
        skip(self)
    )]
    fn into_response(self) -> axum::response::Response {
        // Record the result as part of the current span.
        tracing::Span::current()
            .record("error_stack_trace", self.output_msg())
            .record("status_code", self.status_code().as_u16());

        let code = self.status_code();
        if let Self::Auth { source, .. } = self {
            // no error added into span here and it's Ok
            source.into_response()
        } else {
            let (display_message, debug_message) = self.display_debug_error_messages();
            // Record the result as part of the current span.
            tracing::Span::current().record("display_error", &display_message);
            tracing::Span::current().record("debug_error", &debug_message);
            (
                code,
                Json(ErrorResponse {
                    message: display_message,
                    status_code: code.as_u16(),
                }),
            )
                .into_response()
        }
    }
}

impl Error {
    pub fn display_debug_error_messages(self) -> (String, String) {
        // acquire error str as later it will be moved
        let error_str = self.to_string();
        let debug_str = format!("{self:?}");
        match self {
            Self::QueriesError { source, .. } => match *source {
                crate::queries::Error::Query {
                    source: crate::queries::error::QueryError::Execution { source, .. },
                    ..
                } => SnowflakeError::from(source).display_debug_error_messages(),
                _ => (error_str, debug_str),
            },
            _ => (error_str, debug_str),
        }
    }
}
