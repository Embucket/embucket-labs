use core_metastore::error::MetastoreError;
use core_utils::Error as CoreError;
use datafusion_common::DataFusionError;
use iceberg_s3tables_catalog::error::Error as S3TablesError;
use snafu::Location;
use snafu::prelude::*;
use error_stack_trace;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: MetastoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Core error: {source}"))]
    Core {
        source: CoreError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error: {error}"))]
    DataFusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("S3Tables error: {error}"))]
    S3Tables {
        #[snafu(source)]
        error: S3TablesError,
        #[snafu(implicit)]
        location: Location,
    },
}
