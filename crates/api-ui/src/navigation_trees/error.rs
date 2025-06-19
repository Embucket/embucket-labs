use crate::error::IntoStatusCode;
use core_executor::error::ExecutionError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum NavigationTreesAPIError {
    #[snafu(display("Get navigation trees error: {source}"))]
    Get { source: MetastoreError },

    #[snafu(display("Execution error: {source}"))]
    Execution { source: ExecutionError },
}

// Select which status code to return.
impl IntoStatusCode for NavigationTreesAPIError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}
