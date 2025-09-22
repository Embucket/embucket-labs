use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use snafu::{Location, Snafu};
use std::num::TryFromIntError;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Format must be a non-null scalar value"))]
    FormatMustBeNonNullScalarValue {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported input type: {data_type:?}"))]
    UnsupportedInputType {
        data_type: DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported input type: {data_type:?} on position {position}"))]
    UnsupportedInputTypeWithPosition {
        data_type: DataType,
        position: usize,
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

    #[snafu(display("Invalid boolean string: {v}"))]
    InvalidBooleanString {
        v: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("return_type_from_args should be called"))]
    ReturnTypeFromArgsShouldBeCalled {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid type on {positions:?}"))]
    NoInputArgumentOnPositions {
        positions: Vec<u8>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid precision only allowed from 0 to 38 got: {precision:?}"))]
    InvalidPrecision {
        precision: ScalarValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid scale only allowed from 1 to {precision_minus_one} got: {scale:?}"))]
    InvalidScale {
        precision_minus_one: u8,
        scale: ScalarValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Too little arguments, expected at least: {at_least}, got: {got}"))]
    NotEnoughArguments {
        got: usize,
        at_least: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Too many arguments, expected at maximum: {at_maximum}, got: {got}"))]
    TooManyArguments {
        got: usize,
        at_maximum: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected return type, got: {got:?}, expected similar to: {expected:?}"))]
    UnexpectedReturnType {
        got: DataType,
        expected: DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid integer conversion: {error}"))]
    InvalidIntegerConversion {
        #[snafu(source)]
        error: TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Argument 2 needs to be integer"))]
    ArgumentTwoNeedsToBeInteger {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Argument 2 needs to be string"))]
    ArgumentTwoNeedsToBeString {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid value for function at position 2"))]
    InvalidValueForFunctionAtPositionTwo {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't cast to {v}"))]
    CantCastTo {
        v: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't parse timestamp"))]
    CantParseTimestamp {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't get timestamp"))]
    CantGetTimestamp {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't parse timezone"))]
    CantParseTimezone {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't create DateTime from timestamp"))]
    CantCreateDateTimeFromTimestamp {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't add local timezone"))]
    CantAddLocalTimezone {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid datatype"))]
    InvalidDataType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid type"))]
    InvalidType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported format: {format}. Valid formats are {expected}."))]
    UnsupportedFormat {
        format: String,
        expected: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't format value: {value}. Valid formats are {formats}."))]
    UnsupportedValueFormat {
        value: String,
        formats: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid numeric value: {value}"))]
    InvalidNumericValue {
        value: String,
        #[snafu(source)]
        error: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid int value: {value}"))]
    FailedToParseInt {
        value: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("invalid type [{parameter}({value})] for parameter '{parameter}'"))]
    InvalidTypeForParameter {
        value: String,
        parameter: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast variant value {value} to {real_type}"))]
    FailedToCastVariant {
        value: String,
        real_type: String,
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Conversion
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Conversion {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        UnsupportedInputTypeSnafu {
            data_type: DataType::Boolean,
        }
        .build()
    }
}
