use chrono_tz::ParseError;
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("can't parse date"))]
    CantParseDate {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("timestamp is out of range"))]
    TimestampIsOutOfRange {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("invalid timestamp"))]
    InvalidTimestamp {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("invalid datetime"))]
    InvalidDatetime {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("can't get nanoseconds"))]
    CantGetNanoseconds {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can't parse timezone"))]
    CantParseTimezone {
        #[snafu(source)]
        error: ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't cast to {v}"))]
    CantCastTo {
        v: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid argument: {description}"))]
    InvalidArgument {
        description: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("return_type_from_args should be called"))]
    ReturnTypeFromArgsShouldBeCalled {
        #[snafu(implicit)]
        location: Location,
    },

    // DATEDIFF-specific errors
    #[snafu(display("function requires three arguments"))]
    DateDiffThreeArgsRequired {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Date/time component {found} for function DATEDIFF needs to be an identifier or a string literal."
    ))]
    DateDiffFirstArgNotString {
        found: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Second and third arguments must be Date, Time or Timestamp"))]
    DateDiffSecondAndThirdInvalidTypes {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DATEDIFF does not support mixing TIME with DATE/TIMESTAMP"))]
    DateDiffTimeMixingUnsupported {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid unit type format"))]
    DateDiffInvalidUnitFormat {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "{component} is not a valid date/time component for function DATEDIFF and type TIME."
    ))]
    DateDiffInvalidComponentForTime {
        component: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid date_or_time_part type"))]
    DateDiffInvalidPartType {
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::DateTime
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::DateTime {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        CantParseDateSnafu.build()
    }
}
