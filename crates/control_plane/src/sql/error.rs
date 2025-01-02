use datafusion::error::DataFusionError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError},

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },
}

pub type Result<T> = std::result::Result<T, Error>;