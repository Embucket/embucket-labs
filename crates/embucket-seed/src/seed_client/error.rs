use serde_yaml::Error as SerdeYamlError;
use snafu::prelude::*;
use std::result::Result;

use crate::requests::error::HttpRequestError;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum SeedError {
    #[snafu(display("Error loading seed template: {source}"))]
    LoadSeed { source: SerdeYamlError },

    #[snafu(display("Request error: {source}"))]
    Request { source: HttpRequestError },
}

pub type SeedResult<T> = Result<T, SeedError>;
