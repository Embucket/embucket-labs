use error_stack_trace;
use snafu::{Location, Snafu};
// In this file we create 2 types of errors: DataFusionInternalError and DataFusionExecutionError
// TBD: Basic principles when to use which error type as they look interchangeable
// TBD: May be move errors from this file to a separate crate, like datafusion-external-errors

// Following error is defined with display message to use its message text
// when constructing DataFusionError::Internal, when calling .into conversion.
// This is done to avoid inlining error texts just in code.
// Logically it behaves as transparent error, returning underlying error.
//
// The only problem with that errors, is that location probably can't be used
// as of external nature of following errors
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DataFusionInternalError {
    #[snafu(transparent)]
    Aggregate{
        source: crate::aggregate::Error
    },
    #[snafu(transparent)]
    Conversion{
        source: crate::conversion::Error
    },
    #[snafu(display("Expected JSONPath value for index"))]
    ExpectedJsonPathValueForIndex {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected string array"))]
    ExpectedStringArray {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Unsupported type: {data_type:?}. Only supports boolean, numeric, decimal, float types"
    ))]
    UnsupportedType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("state values should be string type"))]
    StateValuesShouldBeStringType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to deserialize JSON: {error}"))]
    FailedToDeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected UTF8 string for array"))]
    ExpectedUtf8StringForArray {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{argument} argument must be a JSON object"))]
    ArgumentMustBeJsonObject {
        argument: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{argument} argument must be a JSON array"))]
    ArgumentMustBeJsonArray {
        argument: String,
        #[snafu(implicit)]
        location: Location,
    },
    // TODO: Refactor making it more generic
    #[snafu(display(
        "First argument must be a JSON array string, second argument must be a scalar value"
    ))]
    FirstArgumentMustBeJsonArrayStringSecondScalar {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "First argument must be a JSON array string, second argument must be an integer"
    ))]
    FirstArgumentMustBeJsonArrayStringSecondInteger {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "First argument must be a JSON array string, second argument must be an integer, third argument must be a scalar value"
    ))]
    FirstArgumentMustBeJsonArrayStringSecondIntegerThirdScalar {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "First argument must be a JSON array string, second and third arguments must be integers"
    ))]
    FirstArgumentMustBeJsonArrayStringSecondAndThirdIntegers {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("array_cat expects exactly two arguments"))]
    ArrayCatExpectsExactlyTwoArguments {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Cannot concatenate arrays with null values"))]
    CannotConcatenateArraysWithNullValues {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to cast to UTF8: {error}"))]
    FailedToCastToUtf8 {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Array element not found at index"))]
    ArrayElementNotFound {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid array index: {index}"))]
    InvalidArrayIndex {
        index: i64,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Array index out of bounds: {index} (array length: {length})"))]
    ArrayIndexOutOfBounds {
        index: i64,
        length: usize,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected scalar value"))]
    ExpectedScalarValue {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to process array: {error}"))]
    ArrayProcessing {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to cast to {target_type}: {error}"))]
    CastToType {
        target_type: String,
        #[snafu(source)]
        error: datafusion::arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unexpected array type: expected {expected}, got {actual}"))]
    UnexpectedArrayType {
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("LOWER expects 1 argument, got {count}"))]
    LowerExpectsOneArgument {
        count: usize,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid number of arguments"))]
    InvalidNumberOfArguments {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid argument types"))]
    InvalidArgumentTypes {
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DataFusionExecutionError {
    #[snafu(display("Failed to create Tokio runtime: {error}"))]
    FailedToCreateTokioRuntime {
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
    #[cfg(feature = "geospatial")]
    #[snafu(display("Failed to start linestring: {error}"))]
    FailedToStartLinestring {
        #[snafu(source)]
        error: geozero::error::GeozeroError,
        #[snafu(implicit)]
        location: Location,
    },
    #[cfg(feature = "geospatial")]
    #[snafu(display("Failed to end linestring: {error}"))]
    FailedToEndLinestring {
        #[snafu(source)]
        error: geozero::error::GeozeroError,
        #[snafu(implicit)]
        location: Location,
    },
    #[cfg(feature = "geospatial")]
    #[snafu(display("failed to push geom offset: {error}"))]
    FailedToPushGeomOffset {
        #[snafu(source)]
        error: geoarrow::error::GeoArrowError,
        #[snafu(implicit)]
        location: Location,
    },
    #[cfg(feature = "geospatial")]
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
    #[snafu(display("No query found for index {index}"))]
    NoQueryFoundForIndex {
        index: i64,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No result data for query_id {query_id}"))]
    NoResultDataForQueryId {
        query_id: i64,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected SessionState in flatten"))]
    ExpectedSessionStateInFlatten {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected input column to be Utf8"))]
    ExpectedInputColumnToBeUtf8 {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No table found for reference in expression"))]
    NoTableFoundForReferenceInExpression {
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum ArrowInvalidArgumentError {
    #[snafu(display("{data_type} arrays only support Second and Millisecond units, got {unit:?}"))]
    ArraysSupportSecondAndMillisecondUnits {
        data_type: String,
        unit: arrow_schema::TimeUnit,
    },

    #[snafu(display("Expected {data_type} array, got {actual_type:?}"))]
    ExpectedArrayOfType {
        data_type: String,
        actual_type: arrow_schema::DataType,
    },
    #[snafu(display("Unsupported primitive type: {data_type:?}"))]
    UnsupportedPrimitiveType { data_type: arrow_schema::DataType },
}

impl From<DataFusionInternalError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionInternalError) -> Self {
        Self::Internal(value.to_string())
    }
}

impl From<DataFusionExecutionError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionExecutionError) -> Self {
        Self::Execution(value.to_string())
    }
}

impl From<ArrowInvalidArgumentError> for arrow_schema::ArrowError {
    fn from(value: ArrowInvalidArgumentError) -> Self {
        Self::InvalidArgumentError(value.to_string())
    }
}
