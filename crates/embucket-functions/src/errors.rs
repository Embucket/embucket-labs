use error_stack_trace;
use snafu::{Location, Snafu};

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
    #[snafu(display("Expected an array argument"))]
    ArrayArgumentExpected {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected element argument"))]
    ElementArgumentExpected {
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
    #[snafu(display("Missing key column"))]
    MissingKeyColumn {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Key column should be string type"))]
    KeyColumnShouldBeStringType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Missing value column"))]
    MissingValueColumn {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{error}"))]
    SerdeJsonMessage {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Format must be a non-null scalar value"))]
    FormatMustBeNonNullScalarValue {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported input type: {data_type:?}"))]
    UnsupportedInputType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to decode hex string: {error}"))]
    FailedToDecodeHexString {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to decode base64 string: {error}"))]
    FailedToDecodeBase64String {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported format: {format}. Valid formats are HEX, BASE64, and UTF-8"))]
    UnsupportedFormat {
        format: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid boolean string: {v}"))]
    InvalidBooleanString {
        v: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to serialize value to JSON: {error}"))]
    FailedToSerializeValue {
        #[snafu(source)]
        error: serde_json::Error,
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
    #[snafu(display("Failed to deserialize JSON {entity}: {error}"))]
    FailedToDeserializeJsonEntity {
        entity: String,
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Arguments must be JSON arrays"))]
    ArgumentsMustBeJsonArrays {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected array for scalar value"))]
    ExpectedArrayForScalarValue {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{argument} argument must be a JSON array"))]
    ArgumentMustBeJsonArray {
        argument: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{argument} argument must be a scalar value"))]
    ArgumentMustBeAScalarValue {
        argument: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected UTF8 string for array"))]
    ExpectedUtf8StringForArray {
        #[snafu(implicit)]
        location: Location,
    },
    // TODO: Refactor making it more generic
    #[snafu(display(
        "First argument must be a JSON array string, second argument must be a scalar value"
    ))]
    FirstArgumentMustBeJsonArrayStringSecondArgumentMustBeScalarValue {
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
}

impl From<DataFusionInternalError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionInternalError) -> Self {
        Self::Internal(value.to_string())
    }
}

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DataFusionExecutionError {
    #[snafu(display("Input must be a JSON object"))]
    InputMustBeJsonObject {
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<DataFusionExecutionError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionExecutionError) -> Self {
        Self::Execution(value.to_string())
    }
}
