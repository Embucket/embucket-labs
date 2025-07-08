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
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::StringBinary
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::StringBinary {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        Self::ExpectedUtf8String {
            location: location!(),
        }
    }
}
