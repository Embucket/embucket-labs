use crate::error::IntoStatusCode;
use crate::queries::error::QueryError;
use core_metastore::error::MetastoreError;
use http::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DashboardAPIError {
    #[snafu(display("Get total: {source}"))]
    Metastore {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get total: {source}"))]
    Queries {
        source: QueryError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get total: {source}"))]
    History {
        source: core_history::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl IntoStatusCode for DashboardAPIError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}
