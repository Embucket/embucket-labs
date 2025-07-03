use snafu::{Location, Snafu};

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
}

// When directly converting to a DataFusionError
// then crate-level error wouldn't be needed anymore
//
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::DateTime
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::errors::DataFusionExternalError::DateTime {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        CantParseDateSnafu.build()
    }
}
