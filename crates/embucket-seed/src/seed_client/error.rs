use crate::requests::error::HttpRequestError;
use serde_yaml::Error as SerdeYamlError;
use snafu::Location;
use snafu::prelude::*;
use stack_error_proc::stack_trace_debug;
use std::result::Result;

pub type SeedResult<T> = Result<T, SeedError>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum SeedError {
    #[snafu(display("Error loading seed template: {error}"))]
    LoadSeed {
        #[snafu(source)]
        error: SerdeYamlError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Request error: {source}"))]
    Request {
        source: HttpRequestError,
        #[snafu(implicit)]
        location: Location,
    },
}
