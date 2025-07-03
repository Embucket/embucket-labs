use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display(
        "Unsupported type: {data_type:?}. Only supports boolean, numeric, decimal, float types"
    ))]
    UnsupportedType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast to {target_type}"))]
    CastToType {
        target_type: &'static str,
        #[snafu(source)]
        source: datafusion_common::DataFusionError,
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
}

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        datafusion_common::DataFusionError::External(Box::new(value))
    }
}

impl Default for Error {
    fn default() -> Self {
        Self::UnsupportedType {
            data_type: arrow_schema::DataType::Boolean,
            location: snafu::Location::caller(),
        }
    }
}
