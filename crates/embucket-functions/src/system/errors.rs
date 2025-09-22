use snafu::location;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Expected Int64Array"))]
    ExpectedInt64Array {
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::System
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(value))
    }
}

// Default is just a stub, for tests
impl Default for Error {
    fn default() -> Self {
        Self::ExpectedInt64Array {
            location: location!(),
        }
    }
}
