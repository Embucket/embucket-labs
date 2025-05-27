use core_metastore::error::MetastoreError;
use core_utils::Error as CoreError;
use datafusion_common::DataFusionError;
use iceberg_s3tables_catalog::error::Error as S3TablesError;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Metastore error: {source}"))]
    Metastore { source: Box<MetastoreError> },

    #[snafu(display("Core error: {source}"))]
    Core { source: CoreError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("S3Tables error: {source}"))]
    S3Tables { source: Box<S3TablesError> },
}

pub type Result<T> = std::result::Result<T, Error>;
