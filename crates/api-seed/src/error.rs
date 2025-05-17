use snafu::prelude::*;
use std::{result::Result, error::Error};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum SeedError {
    #[snafu(display("Error loading seed data: {source}"))]
    LoadSeed{source: Box<dyn Error>},
}

pub type SeedResult<T> = Result<T, SeedError>;