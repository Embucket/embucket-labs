use snafu::location;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Expected UTF8 string"))]
    ExpectedUtf8String {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected UTF8 string for {name} array"))]
    ExpectedUtf8StringForNamedArray {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast to UTF8: {error}"))]
    FailedToCastToUtf8 {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("LOWER expects 1 argument, got {count}"))]
    LowerExpectsOneArgument {
        count: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "The {function_name} function requires {expected} arguments, but got {actual}"
    ))]
    InvalidArgumentCount {
        function_name: String,
        expected: String,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "The {position} argument of the {function_name} function can only be {expected_type}, but got {actual_type:?}"
    ))]
    InvalidArgumentType {
        function_name: String,
        position: String,
        expected_type: String,
        actual_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to downcast to {array_type}"))]
    FailedToDowncast {
        array_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unsupported data type {data_type:?} for function {function_name}, expected {expected_types}"
    ))]
    UnsupportedDataType {
        function_name: String,
        data_type: String,
        expected_types: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "negative substring length not allowed: {function_name}(<str>, {start}, {length})"
    ))]
    NegativeSubstringLength {
        function_name: String,
        start: i64,
        length: i64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "not enough arguments for function [{function_call}], expected {expected}, got {actual}"
    ))]
    NotEnoughArguments {
        function_call: String,
        expected: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "too many arguments for function [{function_call}] expected {expected}, got {actual}"
    ))]
    TooManyArguments {
        function_call: String,
        expected: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Generated hex string is not valid ASCII"))]
    NonValidASCIIString {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("The following string is not a legal hex-encoded value: {value}"))]
    IllegalHexValue {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Numeric value '{value}' is not recognized"))]
    NumericValueNotRecognized {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid parameter value: {value}. Reason: {reason}."))]
    InvalidParameterValue {
        value: i64,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Input arrays must have the same length"))]
    ArrayLengthMismatch {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("argument {position} to function {function_name} needs to be constant"))]
    NonConstantArgument {
        function_name: String,
        position: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to parse ip: {reason}"))]
    ParseIpFailed {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::StringBinary
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(value))
    }
}

impl Default for Error {
    fn default() -> Self {
        Self::ExpectedUtf8String {
            location: location!(),
        }
    }
}
