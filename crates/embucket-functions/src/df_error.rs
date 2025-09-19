use error_stack_trace;
use snafu::{Location, Snafu};

// Following errors to be converted and returned as a DataFusionError.
// This is done to avoid creating DataFusionError from inline error texts.
//
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum DFExternalError {
    #[snafu(transparent)]
    Aggregate { source: crate::aggregate::Error },
    #[snafu(transparent)]
    Conversion { source: crate::conversion::Error },
    #[snafu(transparent)]
    DateTime { source: crate::datetime::Error },
    #[snafu(transparent)]
    Numeric { source: crate::numeric::Error },
    #[snafu(transparent)]
    StringBinary { source: crate::string_binary::Error },
    #[snafu(transparent)]
    SemiStructured {
        source: crate::semi_structured::Error,
    },
    #[snafu(transparent)]
    Table { source: crate::table::Error },
    // #[cfg(feature = "geospatial")]
    // #[snafu(transparent)]
    // Geospatial { source: crate::geospatial::Error },
    #[snafu(transparent)]
    Crate { source: CrateError },
    #[snafu(transparent)]
    Regexp { source: crate::regexp::Error },
    #[snafu(transparent)]
    System { source: crate::system::Error },
}

impl From<DFExternalError> for datafusion_common::DataFusionError {
    fn from(value: DFExternalError) -> Self {
        Self::External(Box::new(value))
    }
}

// Following enum added for consitent error's structure

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum CrateError {
    #[snafu(display("Failed to create Tokio runtime: {error}"))]
    FailedToCreateTokioRuntime {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Thread panicked while executing future"))]
    ThreadPanickedWhileExecutingFuture {
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
    #[snafu(display("Failed to deserialize JSON: {error}"))]
    FailedToDeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{data_type} arrays only support Second and Millisecond units, got {unit:?}"))]
    ArraysSupportSecondAndMillisecondUnits {
        data_type: String,
        unit: arrow_schema::TimeUnit,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected {data_type} array, got {actual_type:?}"))]
    ExpectedArrayOfType {
        data_type: String,
        actual_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported primitive type: {data_type:?}"))]
    UnsupportedPrimitiveType {
        data_type: arrow_schema::DataType,
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::CrateError
//       |---- Error

impl From<CrateError> for datafusion_common::DataFusionError {
    fn from(value: CrateError) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Crate {
            source: value,
        }))
    }
}
