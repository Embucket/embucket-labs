use datafusion::arrow::datatypes::DataType;
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

    #[snafu(display("Unsupported regex: {error}"))]
    UnsupportedRegex {
        #[snafu(source)]
        error: regex::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported argument value, got: {got}. Expected: {expected}."))]
    UnsupportedArgValue {
        got: String,
        expected: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid parameter value: {got}. Reason: {reason}"))]
    WrongArgValue {
        got: String,
        reason: String,
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
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Regexp
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Regexp {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        FormatMustBeNonNullScalarValueSnafu {}.build()
    }
}
