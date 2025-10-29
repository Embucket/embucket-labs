use crate::error::IntoStatusCode;
use http::StatusCode;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Get navigation trees error: {source}"))]
    Get { source: core_metastore::Error },

    #[snafu(display("Execution error: {source}"))]
    Execution { source: core_executor::Error },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    #[allow(clippy::match_wildcard_for_single_variants)]
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Execution {
                source: core_executor::Error::ConcurrencyLimit { .. },
                ..
            } => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
