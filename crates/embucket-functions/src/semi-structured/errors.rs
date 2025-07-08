use snafu::location;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
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
    ArgumentsMustBothBeEitherScalarUtf8StringsOrArrays {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse JSON: {error}"))]
    JsonParseError {
        #[snafu(source)]
        error: serde_json::Error,
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
    #[snafu(display("Expected array for scalar value"))]
    ExpectedArrayForScalarValue {
        #[snafu(implicit)]
        location: Location,
    },
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

    #[snafu(display("Expected UTF8 string for array"))]
    ExpectedUtf8StringForArray {
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

    #[snafu(display("Arguments must both be either scalar UTF8 strings or arrays"))]
    InvalidArgumentTypesForArrayConcat {
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

    #[snafu(display("Input must be a JSON array"))]
    InputMustBeJsonArray {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected {name} argument"))]
    ExpectedNamedArgument {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mismatched argument types"))]
    MismatchedArgumentTypes {
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

    #[snafu(display("Expected UTF8 string for {name} array"))]
    ExpectedUtf8StringForNamedArray {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("array_generate_range requires 2 or 3 arguments"))]
    ArrayGenerateRangeInvalidArgumentCount {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected {name} argument to be an Int64Array"))]
    ExpectedNamedArgumentToBeAnInt64Array {
        name: String,
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

    #[snafu(display("Failed to parse number"))]
    FailedToParseNumber {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected UTF8 string"))]
    ExpectedUtf8String {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Array argument must be a string type"))]
    ArrayArgumentMustBeAStringType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Element value is null"))]
    ElementValueIsNull {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{name} index must be an integer"))]
    NamedIndexMustBeAnInteger {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Both arguments must be JSON arrays"))]
    BothArgumentsMustBeJsonArrays {
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

    #[snafu(display("object_construct requires an even number of arguments (key-value pairs)"))]
    ObjectConstructRequiresEvenNumberOfArguments {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("object_construct key cannot be null"))]
    ObjectConstructKeyCannotBeNull {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("object_construct key must be a string"))]
    ObjectConstructKeyMustBeAString {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("object_construct value must be a number"))]
    ObjectConstructValueMustBeANumber {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("object_construct value must be a string, number, or boolean"))]
    ObjectConstructValueMustBeAStringNumberOrBoolean {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Key arguments must be scalar values"))]
    KeyArgumentsMustBeScalarValues {
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

    #[snafu(display("{argument} argument must be a scalar value"))]
    ArgumentMustBeAScalarValue {
        argument: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Input must be a JSON object"))]
    InputMustBeJsonObject {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected JSONPath value for index"))]
    ExpectedJsonPathValueForIndex {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid number of arguments"))]
    InvalidNumberOfArguments {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected string array"))]
    ExpectedStringArray {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid argument types"))]
    InvalidArgumentTypes {
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::SemiStructured
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::SemiStructured {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        Self::ArgumentMustBeJsonObject {
            argument: String::new(),
            location: location!(),
        }
    }
}
