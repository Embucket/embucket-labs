use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Format must be a non-null scalar value"))]
    FormatMustBeNonNullScalarValue {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported input type: {data_type:?}"))]
    UnsupportedInputType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode hex string: {error}"))]
    FailedToDecodeHexString {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode base64 string: {error}"))]
    FailedToDecodeBase64String {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported format: {format}. Valid formats are HEX, BASE64, and UTF-8"))]
    UnsupportedFormat {
        format: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid boolean string: {v}"))]
    InvalidBooleanString {
        v: String,
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Conversion
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Conversion {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        UnsupportedInputTypeSnafu {
            data_type: arrow_schema::DataType::Boolean,
        }
        .build()
    }
}
