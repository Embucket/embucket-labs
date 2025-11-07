use datafusion_common::DataFusionError;
use error_stack_trace;
use iceberg_s3tables_catalog::error::Error as S3TablesError;
use snafu::Location;
use snafu::prelude::*;

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

    // TODO: find better place. maybe separate tokio-runtime module
    #[snafu(display("Error creating Tokio runtime: {error}"))]
    CreateTokioRuntime {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Thread panicked while executing future"))]
    ThreadPanickedWhileExecutingFuture {
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
    #[snafu(display("Feature not implemented: {feature:?}, {details:?}"))]
    NotImplemented {
        feature: UnsupportedFeature,
        details: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Missing volume with ident: '{name:?}'"))]
    MissingVolume {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug)]
pub enum UnsupportedFeature {
    DropS3TablesDatabase,
}
