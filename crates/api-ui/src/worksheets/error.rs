use crate::error::IntoStatusCode;
use http::status::StatusCode;
use snafu::Location;
use snafu::prelude::*;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Create worksheet error: {source}"))]
    Create {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get worksheet error: {source}"))]
    Get {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Delete worksheet error: {source}"))]
    Delete {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update worksheet error: {source}"))]
    Update {
        source: WorksheetError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Get worksheets error: {source}"))]
    List {
        source: core_executor::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to parse DateTime: {error}"))]
    Datetime {
        #[snafu(source)]
        error: chrono::ParseError,
        #[snafu(implicit)]
        location: Location,
    },
}

// Kind of reusable worksheet error
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum WorksheetError {
    #[snafu(display("HistoryStore error: {source}"))]
    Store {
        source: core_history::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No fields to update"))]
    NothingToUpdate {
        #[snafu(implicit)]
        location: Location,
    },
}

// Select which status code to return.
impl IntoStatusCode for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Create { source, .. }
            | Self::Get { source, .. }
            | Self::Delete { source, .. }
            | Self::Update { source, .. } => match &source {
                WorksheetError::Store { source, .. } => match source {
                    // use `match self` to return different status_code on the same error
                    core_history::Error::WorksheetAdd { .. } => StatusCode::CONFLICT,
                    core_history::Error::BadKey { .. }
                    | core_history::Error::WorksheetGet { .. }
                    | core_history::Error::WorksheetsList { .. }
                    | core_history::Error::WorksheetUpdate { .. }
                    | core_history::Error::WorksheetDelete { .. } => StatusCode::BAD_REQUEST,
                    core_history::Error::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                WorksheetError::NothingToUpdate { .. } => StatusCode::BAD_REQUEST,
            },
            Self::List { source, .. } => match source {
                core_executor::Error::ConcurrencyLimit { .. } => StatusCode::TOO_MANY_REQUESTS,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Datetime { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
