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

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        datafusion_common::DataFusionError::External(Box::new(value))
    }
}

impl Default for Error {
    fn default() -> Self {
        CantParseDateSnafu.build()
    }
}