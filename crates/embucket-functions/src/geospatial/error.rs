use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use geoarrow::error::GeoArrowError;
use geohash::GeohashError;
use snafu::Snafu;
use snafu::Location;
use error_stack_trace;

pub type GeoDataFusionResult<T> = Result<T, GeoDataFusionError>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum GeoDataFusionError {
    #[snafu(display("Arrow error: {error}"))]
    Arrow {
        #[snafu(source)]
        error: ArrowError,
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

    #[snafu(display("GeoArrow error: {error}"))]
    GeoArrow {
        #[snafu(source)]
        error: GeoArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("GeoHash error: {error}"))]
    GeoHash {
        #[snafu(source)]
        error: GeohashError,
        #[snafu(implicit)]
        location: Location,
    },
}

