use axum::Json;
use axum::response::IntoResponse;
use core_history::QueryRecordId;
use error_stack::ErrorChainExt;
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
        #[snafu(source(from(crate::navigation_trees::Error, Box::new)))]
        source: Box<crate::navigation_trees::Error>,
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
#[serde(rename_all = "camelCase")]
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
        fields(
            display_error,
            debug_error,
            error_stack_trace,
            error_chain,
            status_code
        ),
        skip(self)
    )]
    fn into_response(self) -> axum::response::Response {
        // Record the result as part of the current span.
        tracing::Span::current()
            .record("error_stack_trace", self.output_msg())
            .record("error_chain", self.error_chain())
            .record("query_id", self.query_id().as_i64().to_string())
            .record("query_uuid", self.query_id().as_uuid().to_string())
            .record("status_code", self.status_code().as_u16());

        let code = self.status_code();
        if let Self::Auth { source, .. } = self {
            // no error added into span here and it's Ok
            source.into_response()
        } else {
            let display_message = self.display_error_message();
            // Record the result as part of the current span.
            tracing::Span::current().record("display_error", &display_message);
            tracing::Span::current().record("debug_error", self.debug_error_message());
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
    #[must_use]
    pub fn query_id(&self) -> QueryRecordId {
        match self {
            Self::QueriesError { source, .. } => match source.as_ref() {
                crate::queries::Error::Query {
                    source: crate::queries::error::QueryError::Execution { source, .. },
                    ..
                } => source.query_id(),
                _ => QueryRecordId::default(),
            },
            _ => QueryRecordId::default(),
        }
    }

    #[must_use]
    pub fn display_error_message(&self) -> String {
        // acquire error str as later it will be moved
        let error_str = self.to_string();
        match self {
            Self::QueriesError { source, .. } => match source.as_ref() {
                crate::queries::Error::Query {
                    source: crate::queries::error::QueryError::Execution { source, .. },
                    ..
                } => format!(
                    "{}: {}",
                    source.query_id(),
                    source.to_snowflake_error().display_error_message()
                ),
                _ => error_str,
            },
            _ => error_str,
        }
    }

    #[must_use]
    pub fn debug_error_message(&self) -> String {
        match self {
            Self::QueriesError { source, .. } => match source.as_ref() {
                crate::queries::Error::Query {
                    source: crate::queries::error::QueryError::Execution { source, .. },
                    ..
                } => source.to_snowflake_error().debug_error_message(),
                _ => format!("{self:?}"),
            },
            _ => format!("{self:?}"),
        }
    }
}
