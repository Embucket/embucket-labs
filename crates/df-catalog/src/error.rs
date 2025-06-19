use core_history::errors::Error as HistoryError;
use core_metastore::error::MetastoreError;
use core_utils::Error as CoreError;
use datafusion_common::DataFusionError;
use error_stack_trace;
use iceberg_s3tables_catalog::error::Error as S3TablesError;
use snafu::Location;
use snafu::prelude::*;
use std::{any::Any, fmt};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Metastore error: {source}"))]
    Metastore {
        #[snafu(source(from(MetastoreError, Box::new)))]
        source: Box<MetastoreError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Core error: {source}"))]
    Core {
        #[snafu(source(from(CoreError, Box::new)))]
        source: Box<CoreError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error: {error}"))]
    DataFusion {
        #[snafu(source(from(DataFusionError, Box::new)))]
        error: Box<DataFusionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("S3Tables error: {error}"))]
    S3Tables {
        #[snafu(source(from(S3TablesError, Box::new)))]
        error: Box<S3TablesError>,
        #[snafu(implicit)]
        location: Location,
    },

    // TODO: find better place. maybe separate tokio-runtime module in core-utils ?
    #[snafu(display("Error creating Tokio runtime: {error}"))]
    CreateTokioRuntime {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Thread panicked while executing future with error: {error}"))]
    ThreadPanicked {
        #[snafu(source)]
        error: PanicWrapper,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get worksheets: {error}"))]
    FailedToGetWorksheets {
        #[snafu(source(from(HistoryError, Box::new)))]
        error: Box<HistoryError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to get queries: {error}"))]
    FailedToGetQueries {
        #[snafu(source(from(HistoryError, Box::new)))]
        error: Box<HistoryError>,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug)]
pub struct PanicWrapper(pub Box<dyn Any + Send>);

impl fmt::Display for PanicWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Thread panicked")
    }
}

impl std::error::Error for PanicWrapper {}
