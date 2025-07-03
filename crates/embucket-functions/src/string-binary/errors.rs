use snafu::{Location, Snafu};
use snafu::location;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Expected UTF8 string"))]
    ExpectedUtf8String {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected UTF8 string for array"))]
    ExpectedUtf8StringForArray {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected UTF8 string for {name} array"))]
    ExpectedUtf8StringForNamedArray {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid encoding: {message}"))]
    InvalidEncoding {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("String index out of bounds"))]
    StringIndexOutOfBounds {
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
        Self::ExpectedUtf8String {
            location: location!(),
        }
    }
}
