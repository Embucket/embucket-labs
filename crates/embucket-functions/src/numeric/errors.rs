use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Failed to cast to {target_type}: {error}"))]
    CastToType {
        target_type: String,
        #[snafu(source)]
        error: datafusion::arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Division by zero"))]
    DivisionByZero {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Numeric overflow"))]
    NumericOverflow {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected array type: expected {expected}, got {actual}"))]
    UnexpectedArrayType {
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Numeric
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Numeric {
            source: value,
        }))
    }
}
impl Default for Error {
    fn default() -> Self {
        Self::CastToType {
            target_type: "Float64".to_string(),
            error: datafusion::arrow::error::ArrowError::NotYetImplemented("test".into()),
            location: snafu::location!(),
        }
    }
}
