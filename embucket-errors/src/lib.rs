use snafu::{Backtrace, ErrorCompat, Snafu};
use std::fmt::{Debug, Display};

// 1. Define the EmbucketErrorSource trait
pub trait EmbucketErrorSource: Display + Debug + Send + Sync + 'static {}

// Blanket implementation
impl<T> EmbucketErrorSource for T where T: Display + Debug + Send + Sync + 'static {}


// 2. Define the generic EmbucketError<T> struct
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub struct EmbucketError<T: EmbucketErrorSource> {
    #[snafu(source)] // Indicates that T is the source of this error for std::error::Error::source()
    pub source: T,
    pub context: String,
    backtrace: Backtrace, // Snafu automatically populates this field
}

// Helper function to create an EmbucketError
// Snafu automatically populates the `backtrace` field when an instance of `EmbucketError` is created.
pub fn wrap_error<T: EmbucketErrorSource>(source: T, context_message: String) -> EmbucketError<T> {
    EmbucketError {
        source,
        context: context_message,
        // `backtrace` field is automatically filled by Snafu
    }
}

impl<T: EmbucketErrorSource> EmbucketError<T> {
    pub fn get_backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    // Optional: provide a method that returns Option<&Backtrace>
    // pub fn get_backtrace_opt(&self) -> Option<&Backtrace> {
    //     Some(&self.backtrace) // Since Snafu always populates it, Some is always returned.
    //                           // Could be useful if backtrace capture was conditional.
    // }
}

// To make EmbucketError<T> itself an error that can be propagated with `?`
// or be a source for another Snafu error, it needs to implement std::error::Error.
// T (the source) must also implement std::error::Error for this to work seamlessly.
impl<T> std::error::Error for EmbucketError<T>
where
    T: EmbucketErrorSource + std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source) // The wrapped T is the source
    }
}

// We also need Display for EmbucketError<T>
impl<T: EmbucketErrorSource> Display for EmbucketError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.context, self.source)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    // Removed unused ResultExt import: use snafu::prelude::*;

    // --- Test Error Types ---
    #[derive(Debug, thiserror::Error)]
    enum MyServiceError {
        #[error("Failed to read item: {item_id}")]
        ReadError {
            item_id: String,
            #[source]
            source: std::io::Error,
        },
        #[error("Configuration error: {message}")]
        ConfigError { message: String },
    }

    // A simple String can also be an error source due to the blanket EmbucketErrorSource impl
    // (String implements Display, Debug, Send, Sync, 'static)

    // --- Tests ---

    #[test]
    fn wrap_thiserror_source_with_context() {
        let source_io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let service_error = MyServiceError::ReadError {
            item_id: "test_item".to_string(),
            source: source_io_error,
        };
        let context_msg = "Attempting to process user request".to_string();

        let embucket_err = wrap_error(service_error, context_msg.clone());

        assert_eq!(embucket_err.context, context_msg);
        assert!(format!("{:?}", embucket_err.source).contains("Failed to read item: test_item"));
        assert!(format!("{:?}", embucket_err.source).contains("file not found"));
        
        let backtrace_str = embucket_err.get_backtrace().to_string();
        assert!(!backtrace_str.is_empty(), "Backtrace should not be empty");
        assert!(backtrace_str.contains("embucket_errors"), "Backtrace should mention this crate");

        match &embucket_err.source {
            MyServiceError::ReadError { item_id, source } => {
                assert_eq!(item_id, "test_item");
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            _ => panic!("Incorrect error type"),
        }
    }

    #[test]
    fn wrap_another_thiserror_variant() {
        let config_err = MyServiceError::ConfigError {
            message: "API key missing".to_string(),
        };
        let context_msg = "During application startup".to_string();
        let embucket_err = wrap_error(config_err, context_msg.clone());

        assert_eq!(embucket_err.context, context_msg);
        assert!(format!("{:?}", embucket_err.source).contains("API key missing"));
        let backtrace_str = embucket_err.get_backtrace().to_string();
        assert!(!backtrace_str.is_empty(), "Backtrace should not be empty");
    }

    #[test]
    fn test_embucket_error_display_format() {
        let source_error_msg = "Underlying problem".to_string();
        let context_msg = "Context for the problem".to_string();
        // Using a simple String as the source error
        let embucket_err = wrap_error(source_error_msg.clone(), context_msg.clone());

        let expected_display = format!("{}: {}", context_msg, source_error_msg);
        assert_eq!(embucket_err.to_string(), expected_display);
    }

    #[test]
    fn wrap_simple_string_error() {
        let source_str_error = "A simple string error occurred.".to_string();
        let context_msg = "Wrapping a plain string".to_string();
        let embucket_err = wrap_error(source_str_error.clone(), context_msg.clone());

        assert_eq!(embucket_err.context, context_msg);
        assert_eq!(embucket_err.source, source_str_error);
        let backtrace_str = embucket_err.get_backtrace().to_string();
        assert!(!backtrace_str.is_empty(), "Backtrace should not be empty");
        assert_eq!(embucket_err.to_string(), format!("{}: {}", context_msg, source_str_error));
    }

    #[test]
    fn backtrace_is_captured() {
        let source_err = "test error".to_string();
        let embucket_err = wrap_error(source_err, "context".to_string());
        let backtrace = embucket_err.get_backtrace();
        // A basic check for a captured backtrace. The exact content is highly variable.
        // We primarily check that it's not empty and seems to originate from our test.
        assert!(backtrace.to_string().contains("wrap_simple_string_error") || // if inlined from other test
                backtrace.to_string().contains("backtrace_is_captured") || // if called directly
                backtrace.to_string().contains("embucket_errors::tests"),   // general check
                "Backtrace string should indicate origin within tests. Got: {}", backtrace.to_string()
        );
    }
}

// --- Tests for EmbucketError as a source in another Snafu error ---
// (These were previously in a separate module, can be combined or kept separate)

#[derive(Debug, Snafu)]
enum LayeredAppError {
    #[snafu(display("Critical operation failed: {}", source))]
    CriticalFailure { source: EmbucketError<MyServiceErrorForLayered> },

    #[snafu(display("Another app failure: {}", source))]
    AnotherFailure { source: EmbucketError<SimpleErrorForLayered> }
}

#[derive(Debug, thiserror::Error)]
enum MyServiceErrorForLayered {
    #[error("Layered: Failed to read item: {item_id}")]
    ReadError {
        item_id: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
enum SimpleErrorForLayered {
    #[error("Layered: A simple error occurred")]
    Boom,
}


#[cfg(test)]
mod layered_error_tests {
    use super::*; // Access items from parent module (lib.rs scope)
    use snafu::ErrorCompat; // For extracting backtrace from LayeredAppError if needed
    use snafu::ResultExt; // For .context() selector on Result

    // Re-import test error types if they are not visible or define them here.
    // For this example, ensure MyServiceErrorForLayered and SimpleErrorForLayered are accessible.

    type AppResult<V> = Result<V, LayeredAppError>;

    fn operation_that_might_fail_layered() -> Result<(), EmbucketError<MyServiceErrorForLayered>> {
        let source_io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let service_error = MyServiceErrorForLayered::ReadError {
            item_id: "critical_data".to_string(),
            source: source_io_error,
        };
        Err(wrap_error(
            service_error,
            "Reading critical data for layering".to_string(),
        ))
    }

    fn another_op_might_fail_layered() -> Result<(), EmbucketError<SimpleErrorForLayered>> {
        let simple_error = SimpleErrorForLayered::Boom;
        Err(wrap_error(
            simple_error,
            "Performing simple operation for layering".to_string(),
        ))
    }

    #[test]
    fn test_snafu_context_selector_with_embucket_error() {
        let result: AppResult<()> = operation_that_might_fail_layered().context(CriticalFailureSnafu);

        match result {
            Err(LayeredAppError::CriticalFailure { source }) => { // source is EmbucketError<MyServiceErrorForLayered>
                assert_eq!(source.context, "Reading critical data for layering");
                assert!(source.source.to_string().contains("Layered: Failed to read item: critical_data"));
                
                let embucket_backtrace_str = source.get_backtrace().to_string();
                assert!(!embucket_backtrace_str.is_empty());
                assert!(embucket_backtrace_str.contains("embucket_errors"));

                // Optionally, check the backtrace of the LayeredAppError itself
                let app_error_backtrace = ErrorCompat::backtrace(&LayeredAppError::CriticalFailure { source });
                assert!(app_error_backtrace.is_some());
                assert!(app_error_backtrace.unwrap().to_string().contains("test_snafu_context_selector_with_embucket_error"));


                if let MyServiceErrorForLayered::ReadError { item_id, source: io_err } = &source.source {
                    assert_eq!(item_id, "critical_data");
                    assert_eq!(io_err.kind(), std::io::ErrorKind::PermissionDenied);
                } else {
                    panic!("Incorrect source error type");
                }
            }
            _ => panic!("Expected CriticalFailure"),
        }
    }

    #[test]
    fn test_another_snafu_context_selector_layered() {
        let result: AppResult<()> = another_op_might_fail_layered().context(AnotherFailureSnafu);
         match result {
            Err(LayeredAppError::AnotherFailure { source }) => { // source is EmbucketError<SimpleErrorForLayered>
                assert_eq!(source.context, "Performing simple operation for layering");
                assert!(source.source.to_string().contains("Layered: A simple error occurred"));
                let embucket_backtrace_str = source.get_backtrace().to_string();
                assert!(!embucket_backtrace_str.is_empty());
            }
            _ => panic!("Expected AnotherFailure"),
        }
    }
}
