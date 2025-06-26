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
    ExpectedElementArgument {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected {name} argument"))]
    ExpectedNamedArgument {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected {name} argument to be an Int64Array"))]
    ExpectedNamedArgumentToBeAnInt64Array {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected array for scalar value"))]
    ExpectedArrayForScalarValue {
        #[snafu(implicit)]
        location: Location,
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
    #[snafu(display("Failed to serialize result: {error}"))]
    FailedToSerializeResult {
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
    #[snafu(display("Key arguments must be scalar values"))]
    KeyArgumentsMustBeScalarValues {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Array argument must be a string type"))]
    ArrayArgumentMustBeAStringType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{argument} argument must be a scalar value"))]
    ArgumentMustBeAScalarValue {
        argument: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected UTF8 string"))]
    ExpectedUtf8String {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected UTF8 string for array"))]
    ExpectedUtf8StringForArray {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected UTF8 string for {name} array"))]
    ExpectedUtf8StringForNamedArray {
        name: String,
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
    #[snafu(display("Arguments must both be either scalar UTF8 strings or arrays"))]
    InvalidArgumentTypesForArrayConcat {
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
    #[snafu(display("Element value is null"))]
    ElementValueIsNull {
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
    #[snafu(display("Both arguments must be JSON arrays"))]
    BothArgumentsMustBeJsonArrays {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Input must be a JSON array"))]
    InputMustBeJsonArray {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Mismatched argument types"))]
    MismatchedArgumentTypes {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("array_generate_range requires 2 or 3 arguments"))]
    ArrayGenerateRangeInvalidArgumentCount {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Position {pos} is out of bounds for array of length {length}"))]
    PositionOutOfBoundsForArrayLength {
        pos: usize,
        length: usize,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Position must be an integer"))]
    PositionMustBeAnInteger {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{name} index must be an integer"))]
    NamedIndexMustBeAnInteger {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("All arguments must be arrays"))]
    AllArgumentsMustBeArrays {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Mixed scalar and array arguments are not supported"))]
    MixedScalarAndArrayArgumentsNotSupported {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ARRAYS_ZIP requires at least one array argument"))]
    ArraysZipRequiresAtLeastOneArrayArgument {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("LOWER expects 1 argument, got {count}"))]
    LowerExpectsOneArgument {
        count: usize,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to parse number"))]
    FailedToParseNumber {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Key must be a string"))]
    KeyMustBeAString {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Key '{name}' already exists and update_flag is false"))]
    KeyAlreadyExistsAndUpdateFlagIsFalse {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Update flag must be a boolean"))]
    UpdateFlagMustBeABoolean {
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
