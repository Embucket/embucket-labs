use error_stack_trace;
use snafu::{Location, Snafu};

// Following error is defined with display message to use its message text
// when constructing DataFusionError::Internal, when calling .into conversion.
// This is done to avoid inlining error texts just in code.
// Logically it behaves as transparent error, returning underlying error.
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DataFusionInternalError {
    #[snafu(display("Expected an array argument"))]
    ArrayArgumentExpected {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Unsupported type: {data_type:?}. Only supports boolean, numeric, decimal, float types"
    ))]
    UnsupportedType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("state values should be string type"))]
    StateValuesShouldBeStringType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Missing key column"))]
    MissingKeyColumn {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Key column should be string type"))]
    KeyColumnShouldBeStringType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Missing value column"))]
    MissingValueColumn {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{error}"))]
    SerdeJsonMessage {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
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
    #[snafu(display("Failed to serialize JSON value: {error}"))]
    FailedToSerializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<DataFusionInternalError> for datafusion_common::DataFusionError {
    fn from(value: DataFusionInternalError) -> Self {
        Self::Internal(value.to_string())
    }
}
