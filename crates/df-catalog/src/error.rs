use core_metastore::error::MetastoreError;
use core_utils::Error as CoreError;
use datafusion_common::DataFusionError;
use iceberg_s3tables_catalog::error::Error as S3TablesError;
use snafu::Location;
use snafu::prelude::*;
use stack_error::StackError;
use stack_error_proc::stack_trace_debug;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        source: Box<MetastoreError>,
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
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("S3Tables error: {error}"))]
    S3Tables {
        error: Box<S3TablesError>,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
