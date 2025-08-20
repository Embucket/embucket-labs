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
