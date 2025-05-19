use api_client_rest::error::HttpRequestError;
use snafu::prelude::*;
use std::{error::Error, result::Result};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum SeedError {
    #[snafu(display("Error loading seed data: {source}"))]
    LoadSeed { source: Box<dyn Error> },

    #[snafu(display("Request error: {source}"))]
    Request { source: HttpRequestError },
}

pub type SeedResult<T> = Result<T, SeedError>;
