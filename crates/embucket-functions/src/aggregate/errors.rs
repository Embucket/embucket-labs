use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
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

    #[snafu(display("State values should be string type"))]
    StateValuesShouldBeStringType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse boolean value"))]
    FailedToParseBoolean {
        #[snafu(source)]
        error: std::str::ParseBoolError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse float value"))]
    FailedToParseFloat {
        #[snafu(source)]
        error: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse float value"))]
    FailedToParseFloatNoSource {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ARRAY_UNION_AGG only supports JSON array input"))]
    ArrayUnionAggOnlySupportsJsonArray {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ARRAY_UNION_AGG only supports boolean, float64, and UTF-8 string types"))]
    ArrayUnionAggOnlySupportsBooleanFloat64AndUtf8 {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to serialize value to JSON: {error}"))]
    FailedToSerializeValue {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
//
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Aggregate
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Aggregate {
            source: value,
        }))
    }
}

impl Default for Error {
    fn default() -> Self {
        MissingValueColumnSnafu.build()
    }
}
