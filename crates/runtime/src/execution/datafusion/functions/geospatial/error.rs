use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use geoarrow::error::GeoArrowError;
use geohash::GeohashError;
use snafu::Snafu;
use std::fmt::Debug;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum GeoDataFusionError {
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("GeoArrow error: {source}"))]
    GeoArrow { source: GeoArrowError },

    #[snafu(display("GeoHash error: {source}"))]
    GeoHash { source: GeohashError },
}

pub type GeoDataFusionResult<T> = Result<T, GeoDataFusionError>;

impl From<GeoDataFusionError> for DataFusionError {
    fn from(value: GeoDataFusionError) -> Self {
        match value {
            GeoDataFusionError::Arrow { source } => Self::ArrowError(source, None),
            GeoDataFusionError::DataFusion { source } => source,
            GeoDataFusionError::GeoArrow { source } => Self::External(Box::new(source)),
            GeoDataFusionError::GeoHash { source } => Self::External(Box::new(source)),
        }
    }
}
