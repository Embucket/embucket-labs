use error_stack_trace;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum ArrowInvalidArgumentError {
    #[snafu(display("{data_type} arrays only support Second and Millisecond units, got {unit:?}"))]
    ArraysSupportSecondAndMillisecondUnits {
        data_type: String,
        unit: datafusion::arrow::datatypes::TimeUnit,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected {data_type} array, got {actual_type:?}"))]
    ExpectedArrayOfType {
        data_type: String,
        actual_type: datafusion::arrow::datatypes::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported primitive type: {data_type:?}"))]
    UnsupportedPrimitiveType {
        data_type: datafusion::arrow::datatypes::DataType,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<ArrowInvalidArgumentError> for datafusion::arrow::error::ArrowError {
    fn from(value: ArrowInvalidArgumentError) -> Self {
        Self::InvalidArgumentError(value.to_string())
    }
}
