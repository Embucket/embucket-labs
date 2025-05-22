use snafu::prelude::*;
use embucket_errors::{EmbucketError, EmbucketErrorSource, wrap_error}; // Corrected import

// Errors from other crates that CliError might wrap
use core_metastore::error::MetastoreError;
use core_executor::error::ExecutionError;
// Add other specific error types from your project or external crates as needed
// For example:
// use api_iceberg_rest::error::IcebergAPIError;
use std::io::Error as StdIoError;
use serde_yaml::Error as SerdeYamlError;
use tokio::task::JoinError as TokioJoinError;
use core_utils::Error as CoreUtilsError; // From core-utils, if used directly for errors

// --- Define CLI Application's own specific error kinds ---
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CliErrorKind {
    #[snafu(display("Failed to load configuration from '{path}': {source}"))]
    ConfigLoad { path: String, source: SerdeYamlError }, // Example: if config is YAML

    #[snafu(display("Failed to load configuration from '{path}': {message}"))]
    ConfigLoadIo { path: String, message: String }, // For IO errors reading config

    #[snafu(display("Invalid command line argument: {message}"))]
    InvalidCliArgument { message: String },

    #[snafu(display("Initialization of service '{service_name}' failed"))]
    InitializationFailed { service_name: String },

    #[snafu(display("A required environment variable '{name}' is not set or invalid: {reason}"))]
    MissingEnvVar { name: String, reason: String },

    #[snafu(display("Failed to initialize tracing subscriber: {message}"))]
    TracingSetup { message: String },
}

// --- Main CliError Enum ---
// Each variant will typically hold an EmbucketError wrapping a more specific error
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CliError {
    #[snafu(display("Metastore operation failed: {}", source))]
    Metastore { source: EmbucketError<MetastoreError> },

    #[snafu(display("Execution engine operation failed: {}", source))]
    Executor { source: EmbucketError<ExecutionError> },

    // Example for API errors if main directly interacts or handles them
    // #[snafu(display("API Error: {}", source))]
    // ApiError { source: EmbucketError<IcebergAPIError> },

    #[snafu(display("I/O error: {}", source))]
    Io { source: EmbucketError<StdIoError> },

    #[snafu(display("Configuration parsing error: {}", source))]
    ConfigParsing { source: EmbucketError<SerdeYamlError> },
    
    #[snafu(display("Core utilities error: {}", source))]
    Utils { source: EmbucketError<CoreUtilsError> },

    #[snafu(display("Async task execution failed: {}", source))]
    AsyncTask { source: EmbucketError<TokioJoinError> },

    // This variant wraps CliErrorKind for errors originating within this binary's logic
    #[snafu(display("Application error: {}", source))]
    Application { source: EmbucketError<CliErrorKind> },
}

// Result type alias for main.rs and other modules in this crate
pub type CliResult<T> = Result<T, CliError>;

// --- Helper functions for creating CliError (Optional, but can be useful) ---
// Example:
// pub fn config_load_error(path: String, source: SerdeYamlError) -> CliError {
//     let kind = CliErrorKind::ConfigLoad { path, source };
//     let embucket_error = wrap_error(kind, "Configuration loading failed".to_string());
//     CliError::Application { source: embucket_error }
// }

// The Display trait is automatically implemented by Snafu.
// When a CliError is displayed, it will show:
// For `CliError::Application { source }`: "Application error: {source}"
//   where `{source}` (EmbucketError<CliErrorKind>) displays as "{context}: {CliErrorKind_display}"
// For `CliError::Io { source }`: "I/O error: {source}"
//   where `{source}` (EmbucketError<StdIoError>) displays as "{context}: {StdIoError_display}"
// This provides a layered error message.

// Backtraces:
// Each EmbucketError instance captures a backtrace.
// To access it:
// if let Err(e) = result {
//     match e {
//         CliError::Application { source, .. } => {
//             if let Some(bt) = source.backtrace() { // Assuming EmbucketError has a backtrace() method
//                 eprintln!("Backtrace:\n{}", bt);
//             }
//         }
//         // ... other variants
//     }
// }
// Note: The `embucket-errors` crate's `EmbucketError` struct must provide a method to access its `Backtrace`.
// If `EmbucketError` is `#[derive(Snafu)]`, then `snafu::ErrorCompat::backtrace(&source)` can get it.
// Let's assume `embucket_errors::EmbucketError` has a `pub fn backtrace(&self) -> Option<&Backtrace>` method or similar.
// Or, if `EmbucketError` itself implements `std::error::Error`, then `ErrorCompat::backtrace(cli_error_instance)`
// might give the outermost backtrace. We want the backtrace associated with the specific `EmbucketError` layer.
// The `EmbucketError` definition from `embucket-errors/src/lib.rs` has `pub fn get_backtrace(&self) -> &Backtrace`.
// So, for `CliError::Application { source }`, `source.get_backtrace()` would be used.
// For `CliError::Io { source }`, `source.get_backtrace()` would be used.
// This ensures we get the backtrace associated with the context where `wrap_error` was called.
// If `CliError` itself needs to be a source for another error and carry its own distinct backtrace,
// it would also need a `backtrace: Backtrace` field populated by Snafu. But here, it's the top-level error.
// The user of `CliResult` will handle printing the error and potentially the backtrace.
// A common pattern for main.rs:
//
// fn main() -> ExitCode {
//     if let Err(e) = run_app() {
//         eprintln!("Error: {}", e);
//         if std::env::var("RUST_BACKTRACE").map_or(false, |val| val == "1" || val == "full") {
//              // Access backtrace from 'e' if available and print
//              // This part requires careful handling of which backtrace to print.
//              // Generally, the one from the EmbucketError that is the direct source of the CliError variant.
//              // e.g., if e is CliError::Application { source: embucket_cli_error_kind }, print source.get_backtrace().
//         }
//         return ExitCode::FAILURE;
//     }
//     ExitCode::SUCCESS
// }
//
// fn run_app() -> CliResult<()> { /* ... */ }

#[cfg(test)]
mod tests {
    use super::*;
    use embucket_errors::wrap_error; // For constructing EmbucketError for tests
    use serde_yaml; // For the SerdeYamlError in CliErrorKind::ConfigLoad

    // Helper to create a dummy SerdeYamlError for testing ConfigLoad
    fn create_dummy_serde_yaml_error() -> serde_yaml::Error {
        // serde_yaml::Error is complex to construct directly.
        // A common way it's created is from io or parsing issues.
        // We'll simulate a simple one from a parsing error.
        serde_yaml::from_str::<i32>("---invalid yaml---").unwrap_err()
    }

    #[test]
    fn test_cli_error_display_application_config_load() {
        let path = "/path/to/config.yaml".to_string();
        let serde_err = create_dummy_serde_yaml_error();
        let cli_kind_err = CliErrorKind::ConfigLoad { path: path.clone(), source: serde_err };
        
        let app_context = "Failed to initialize application due to config issue".to_string();
        let embucket_err = wrap_error(cli_kind_err, app_context.clone());
        
        let cli_error = CliError::Application { source: embucket_err };

        let display_str = cli_error.to_string();
        
        // Expected: "Application error: {app_context}: Failed to load configuration from '{path}': {serde_err_string}"
        assert!(display_str.starts_with("Application error: "));
        assert!(display_str.contains(&app_context));
        assert!(display_str.contains(&format!("Failed to load configuration from '{}'", path)));
        // Check for part of the serde_yaml error message
        assert!(display_str.contains("invalid type: string \"---invalid yaml---\", expected i32") || display_str.contains("invalid type: string"));


        // Test backtrace accessibility (basic check)
        match cli_error {
            CliError::Application { source } => {
                assert!(!source.get_backtrace().to_string().is_empty(), "Backtrace should be captured");
            }
            _ => panic!("Incorrect CliError variant"),
        }
    }

    #[test]
    fn test_cli_error_display_io_error() {
        let underlying_io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "Original OS error: file not found");
        let io_context = "While trying to read critical file '/my/file.txt'".to_string();
        let embucket_io_err = wrap_error(underlying_io_error, io_context.clone());
        
        let cli_error = CliError::Io { source: embucket_io_err };

        let display_str = cli_error.to_string();

        // Expected: "I/O error: {io_context}: Original OS error: file not found"
        assert!(display_str.starts_with("I/O error: "));
        assert!(display_str.contains(&io_context));
        assert!(display_str.contains("Original OS error: file not found"));

        match cli_error {
            CliError::Io { source } => {
                assert!(!source.get_backtrace().to_string().is_empty(), "Backtrace should be captured");
            }
            _ => panic!("Incorrect CliError variant"),
        }
    }

    #[test]
    fn test_cli_error_display_initialization_failed() {
        let service_name = "FancyWebService".to_string();
        let cli_kind_err = CliErrorKind::InitializationFailed { service_name: service_name.clone() };
        let app_context = "Startup sequence aborted".to_string();
        let embucket_err = wrap_error(cli_kind_err, app_context.clone());
        let cli_error = CliError::Application { source: embucket_err };

        let display_str = cli_error.to_string();

        // Expected: "Application error: {app_context}: Initialization of service '{service_name}' failed"
        assert!(display_str.starts_with("Application error: "));
        assert!(display_str.contains(&app_context));
        assert!(display_str.contains(&format!("Initialization of service '{}' failed", service_name)));
    }
}
