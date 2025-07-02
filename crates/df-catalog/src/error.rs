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
        #[snafu(source(from(core_metastore::Error, Box::new)))]
        source: Box<core_metastore::Error>,
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
        #[snafu(source(from(core_history::Error, Box::new)))]
        error: Box<core_history::Error>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to get queries: {error}"))]
    FailedToGetQueries {
        #[snafu(source(from(core_history::Error, Box::new)))]
        error: Box<core_history::Error>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid cache: Can't locate '{entity:?}' entity = {name}"))]
    InvalidCache {
        entity: String,
        name: String,
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

// This errors list created from inlined errors texts
// Not sure how location and error_stack_tarce of this error could be used in future
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DataFusionExecutionError {
    #[snafu(display("Object store not found for url {url}"))]
    ObjectStoreNotFound {
        url: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Ordinal position param overflow: {error}"))]
    OrdinalPositionParamOverflow {
        #[snafu(source)]
        error: std::num::TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("rid param doesn't fit in u8"))]
    RidParamDoesntFitInU8 {
        #[snafu(source)]
        error: std::num::TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Core history error: {error}"))]
    DFExecutionCoreHistory {
        #[snafu(source)]
        error: core_history::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Core utils error: {error}"))]
    DFExecutionCoreUtils {
        #[snafu(source)]
        error: core_utils::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<DataFusionExecutionError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionExecutionError) -> Self {
        Self::Internal(value.to_string())
    }
}

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DataFusionPlanError {
    #[snafu(display("Catalog '{name}' not found in catalog list"))]
    DFPlanErrorCatalogNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<DataFusionPlanError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionPlanError) -> Self {
        Self::Internal(value.to_string())
    }
}
