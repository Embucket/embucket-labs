use crate::error as errors;
use datafusion::error::DataFusionError;
use datafusion_common::{Diagnostic, Location, Span};
use embucket_functions::df_error::DFExternalError;
use snafu::location;

#[allow(clippy::unwrap_used)]
#[test]
fn test_datafusion_errors() {
    // Internal error
    let err = DataFusionError::Internal("1".into());
    assert_eq!(
        err.to_string(),
        "Internal error: 1.\nThis was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker"
    );

    // Execution error
    let err = DataFusionError::Execution("1".into());
    assert_eq!(err.to_string(), "Execution error: 1");

    // Plan error
    let err = DataFusionError::Plan("1".into());
    assert_eq!(err.to_string(), "Error during planning: 1");

    // External error
    let err = DataFusionError::External(Box::new(DataFusionError::NotImplemented("1".into())));
    assert_eq!(
        err.to_string(),
        "External error: This feature is not implemented: 1"
    );

    // Extrernal error with downcast
    let err = DataFusionError::External(Box::new(DFExternalError::Numeric {
        source: embucket_functions::numeric::Error::CastToType {
            target_type: "int".to_string(),
            error: datafusion::arrow::error::ArrowError::DivideByZero,
            location: location!(),
        },
    }));
    assert_eq!(
        err.to_string(),
        "External error: Failed to cast to int: Divide by zero error"
    );
}

#[test]
fn test_error_not_supported() {
    let err = errors::Error::DataFusion {
        error: Box::new(DataFusionError::NotImplemented("1".into())),
        location: location!(),
    }
    .to_snowflake_error();

    if !err
        .to_string()
        .starts_with("SQL compilation error: unsupported feature")
    {
        panic!("Actual error: {err}");
    }
}

#[test]
fn test_error_diagnostic_location() {
    let err = errors::Error::DataFusion {
        error: Box::new(DataFusionError::Diagnostic(
            Box::new(Diagnostic::new_error(
                "err",
                Some(Span::new(
                    Location { line: 1, column: 2 },
                    Location { line: 1, column: 3 },
                )),
            )),
            Box::new(DataFusionError::Plan("plan".into())),
        )),
        location: location!(),
    }
    .to_snowflake_error();
    if !err
        .to_string()
        .starts_with("SQL compilation error: error line 1 at position 2\n")
    {
        panic!("Actual error: {err}");
    }
}
