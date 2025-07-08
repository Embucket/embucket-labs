use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use error_stack_trace;
use geoarrow::error::GeoArrowError;
use geohash::GeohashError;
use snafu::Location;
use snafu::Snafu;

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


#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("ST_Contains does not support this left geometry type"))]
    STContainsDoesNotSupportThisLeftGeometryType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ST_Distance does not support this left geometry type"))]
    STDistanceDoesNotSupportThisLeftGeometryType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ST_Distance does not support this right geometry type"))]
    STDistanceDoesNotSupportThisRightGeometryType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ST_Within does not support this left geometry type"))]
    STWithinDoesNotSupportThisLeftGeometryType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unexpected input data type: {data_type}"))]
    UnexpectedInputDataType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected only one argument in ST_Dimension"))]
    ExpectedOnlyOneArgumentInSTDimension {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported geometry type"))]
    UnsupportedGeometryType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Null geometry found"))]
    NullGeometryFound {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected only one argument in ST_SRID"))]
    ExpectedOnlyOneArgumentInSTSRID {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected only one argument"))]
    ExpectedOnlyOneArgument {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected at least one argument"))]
    ExpectedAtLeastOneArgument {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Error getting bounding rect: {error}"))]
    ErrorGettingBoundingRect {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Index out of bounds"))]
    IndexOutOfBounds {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected two arguments in ST_PointN"))]
    ExpectedTwoArgumentsInSTPointN {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected Geometry-typed array"))]
    ExpectedGeometryTypedArray {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ST_Within takes two arguments"))]
    STWithinTakesTwoArguments {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ST_Within does not support this rhs geometry type"))]
    STWithinDoesNotSupportThisRhsGeometryType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Coordinate is None"))]
    CoordinateIsNone {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected Point, LineString, or MultiPoint in ST_Makeline"))]
    ExpectedPointLineStringOrMultiPointInSTMakeLine {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected only one argument in ST_MakePolygon"))]
    ExpectedOnlyOneArgumentInSTMakePolygon {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to start linestring: {error}"))]
    FailedToStartLinestring {
        #[snafu(source)]
        error: geozero::error::GeozeroError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to end linestring: {error}"))]
    FailedToEndLinestring {
        #[snafu(source)]
        error: geozero::error::GeozeroError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("failed to push geom offset: {error}"))]
    FailedToPushGeomOffset {
        #[snafu(source)]
        error: geoarrow::error::GeoArrowError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("failed to add coord: {error}"))]
    FailedToAddCoord {
        #[snafu(source)]
        error: geoarrow::error::GeoArrowError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected only one argument in ST_Area"))]
    ExpectedOnlyOneArgumentInSTArea {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ST_Contains does not support this rhs geometry type"))]
    STContainsDoesNotSupportThisRhsGeometryType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ST_Contains takes two arguments"))]
    STContainsTakesTwoArguments {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ST_Distance does not support this rhs geometry type"))]
    STDistanceDoesNotSupportThisRhsGeometryType {
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Geospatial
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        datafusion_common::DataFusionError::External(Box::new(
            crate::errors::DFExternalError::Geospatial{source: value}
        ))
    }
}