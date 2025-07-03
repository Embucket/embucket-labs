use error_stack_trace;
use snafu::{Location, Snafu};

// Following Error to be converted into DataFusionError

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("No query found for index {index}"))]
    NoQueryFoundForIndex {
        index: i64,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No result data for query_id {query_id}"))]
    NoResultDataForQueryId {
        query_id: i64,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Expected SessionState in flatten"))]
    ExpectedSessionStateInFlatten {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected input column to be Utf8"))]
    ExpectedInputColumnToBeUtf8 {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No table found for reference in expression"))]
    NoTableFoundForReferenceInExpression {
        #[snafu(implicit)]
        location: Location,
    },
}

// Enum variants from this error return DataFusionError
// Following is made to preserve logical structure of error:
// DataFusionError::External
// |---- DataFusionInternalError::Table
//       |---- Error

impl From<Error> for datafusion_common::DataFusionError {
    fn from(value: Error) -> Self {
        Self::External(Box::new(crate::df_error::DFExternalError::Table {
            source: value,
        }))
    }
}
