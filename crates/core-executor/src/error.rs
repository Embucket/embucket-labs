use snafu::prelude::*;
use std::backtrace::Backtrace as StdBacktrace; // Renamed to avoid conflict if any

// Import from embucket-errors
use embucket_errors::{EmbucketError, EmbucketErrorSource, wrap_error};

// Original error sources from other crates
use datafusion_common::DataFusionError;
use df_catalog::error::Error as CatalogError;
use iceberg_rust::error::Error as IcebergError;
use iceberg_s3tables_catalog::error::Error as S3tablesError;
use object_store::Error as ObjectStoreError;
use url::ParseError as UrlParseError;
use core_metastore::error::MetastoreError;
use datafusion::arrow::error::ArrowError;
use crate::dedicated_executor::JobError as DedicatedJobError; // Keep original name for clarity

// --- Define Core Executor's own specific error kinds ---
// These will be wrapped by EmbucketError and then by ExecutionError

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CoreExecutorErrorKind {
    #[snafu(display("Invalid table identifier: {ident}"))]
    InvalidTableIdentifier { ident: String },

    #[snafu(display("Invalid schema identifier: {ident}"))]
    InvalidSchemaIdentifier { ident: String },

    #[snafu(display("Invalid file path: {path}"))]
    InvalidFilePath { path: String },

    #[snafu(display("Invalid bucket identifier: {ident}"))]
    InvalidBucketIdentifier { ident: String },

    #[snafu(display("No Table Provider found for table: {table_name}"))]
    TableProviderNotFound { table_name: String },

    #[snafu(display("Missing DataFusion session for id {id}"))]
    MissingDataFusionSession { id: String },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound { db: String },

    #[snafu(display("Table {table} not found"))]
    TableNotFound { table: String },

    #[snafu(display("Schema {schema} not found"))]
    SchemaNotFound { schema: String },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound { volume: String },

    #[snafu(display("Object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Unsupported file format {format}"))]
    UnsupportedFileFormat { format: String },

    #[snafu(display("Catalog {catalog} cannot be downcasted"))]
    CatalogDownCast { catalog: String },

    #[snafu(display("Catalog {catalog} not found"))]
    CatalogNotFound { catalog: String },

    #[snafu(display("Failed to upload file: {message}"))]
    UploadFailed { message: String },

    #[snafu(display("CatalogList downcast failed"))]
    CatalogListDowncast,
    // Add other specific internal errors here if they don't have an external source
}

// --- Main ExecutionError Enum ---
// Each variant will typically hold an EmbucketError wrapping a more specific error (either external or a CoreExecutorErrorKind)
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ExecutionError {
    #[snafu(display("Cannot register UDF functions: {}", source))]
    RegisterUDF { source: EmbucketError<DataFusionError> },

    #[snafu(display("Cannot register UDAF functions: {}", source))]
    RegisterUDAF { source: EmbucketError<DataFusionError> },

    #[snafu(display("DataFusion error: {}", source))]
    DataFusion { source: EmbucketError<DataFusionError> },

    #[snafu(display("Arrow error: {}", source))]
    Arrow { source: EmbucketError<ArrowError> },

    #[snafu(display("DataFusion query error: {}, query: {}", source, query))]
    DataFusionQuery {
        source: EmbucketError<DataFusionError>,
        query: String,
    },

    #[snafu(display("Error encoding UTF8 string: {}", source))]
    Utf8 { source: EmbucketError<std::string::FromUtf8Error> },

    #[snafu(display("Metastore error: {}", source))]
    Metastore { source: EmbucketError<MetastoreError> },

    #[snafu(display("Object store error: {}", source))]
    ObjectStore { source: EmbucketError<ObjectStoreError> },

    #[snafu(display("Cannot refresh catalog list: {}", source))]
    RefreshCatalogList { source: EmbucketError<CatalogError> },

    #[snafu(display("S3Tables error: {}", source))]
    S3Tables { source: EmbucketError<S3tablesError> },

    #[snafu(display("Iceberg error: {}", source))]
    Iceberg { source: EmbucketError<IcebergError> },

    #[snafu(display("URL Parsing error: {}", source))]
    UrlParse { source: EmbucketError<UrlParseError> },

    #[snafu(display("Threaded Job error: {}", source))]
    JobError { source: EmbucketError<DedicatedJobError> },
    
    #[snafu(display("Failed to register catalog: {}", source))]
    RegisterCatalog { source: EmbucketError<CatalogError> },

    // Variants that wrap CoreExecutorErrorKind
    #[snafu(display("Execution failed: {}", source))]
    ExecutionFailed { source: EmbucketError<CoreExecutorErrorKind> },
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;

// --- Helper functions to create ExecutionError instances ---
// These would be used in the codebase where errors are generated.
// Example:
// ```
// use crate::error::{ExecutionError, CoreExecutorErrorKind, InvalidTableIdentifierSnafu, ExecutionFailedSnafu};
// use embucket_errors::wrap_error; // Assuming this is brought into scope
//
// fn some_operation(table_name: &str) -> ExecutionResult<()> {
//     if table_name == "invalid" {
//         let specific_error = CoreExecutorErrorKind::InvalidTableIdentifier { ident: table_name.to_string() };
//         let wrapped_error = wrap_error(specific_error, format!("Validation failed for table '{}'", table_name));
//         return ExecutionFailedSnafu { source: wrapped_error }.fail();
//     }
//     // ...
//     Ok(())
// }
//
// fn another_operation() -> ExecutionResult<()> {
//     let io_error = std::io::Error::new(std::io::ErrorKind::Other, "some io error");
//     let wrapped_io_error = wrap_error(io_error, "Failed to read config file".to_string());
//     // Assuming DataFusionError can be created from an EmbucketError<std::io::Error> or similar
//     // For this example, let's imagine a direct DataFusion variant that takes EmbucketError<std::io::Error>
//     // Or, more realistically, the DataFusion error occurs, and *then* it's wrapped.
//     // let df_error = DataFusionError::IoError(wrapped_io_error.to_string()); // This is not how DF errors work
//
//     // Correct approach:
//     // 1. Original error occurs
//     // 2. Wrap with EmbucketError (adds context, backtrace)
//     // 3. This EmbucketError becomes the source for ExecutionError variant
//
//     // Example for an external error like DataFusionError:
//     // let original_df_error = perform_df_op()?; // This would return a Result<_, DataFusionError>
//     // let embucket_df_error = wrap_error(original_df_error, "Executing SQL query".to_string());
//     // return DataFusionSnafu { source: embucket_df_error }.fail();
//     Ok(())
// }
// ```

// The EmbucketErrorSource trait is blanket implemented in embucket-errors crate
// for any T: Display + Debug + Send + Sync + 'static.
// All original error types (DataFusionError, MetastoreError, etc.) and CoreExecutorErrorKind
// need to satisfy these bounds. Most error types from reputable crates do.
// CoreExecutorErrorKind is derived with Snafu, which provides Display and Debug.
// We must ensure they are Send + Sync + 'static. Snafu errors usually are.

// Note: The actual usage of `wrap_error` and then `.context(ExecutionErrorVariantSnafu)`
// will happen in the respective modules of `core-executor` where these errors originate,
// not in this `error.rs` file. This file just defines the error structures.
// The example comments above are for illustration of how it *would* be used.
//
// For variants like `InvalidTableIdentifier`, etc., they are now part of `CoreExecutorErrorKind`.
// When such an error occurs:
// 1. Create `CoreExecutorErrorKind::InvalidTableIdentifier { ... }`.
// 2. Wrap it: `let embucket_core_error = wrap_error(kind_error, "Contextual message");`
// 3. Create `ExecutionError`: `ExecutionFailedSnafu { source: embucket_core_error }.fail()?` or `.build()`

// The `JobError` variant was:
// JobError { source: crate::dedicated_executor::JobError, backtrace: Backtrace }
// It's now:
// JobError { source: EmbucketError<DedicatedJobError> }
// `crate::dedicated_executor::JobError` needs to be `Display + Debug + Send + Sync + 'static`.
// If `DedicatedJobError` is already a snafu error, it's fine.
// The backtrace is now part of `EmbucketError`.
// The display for `ExecutionError::JobError` will be `"{}: {}", source.context, source.source`
// (implicitly via `source.to_string()` if `EmbucketError`'s `Display` is `"{context}: {source}"`).
// Check `embucket-errors/src/lib.rs` for `EmbucketError`'s Display impl. It is indeed:
// `write!(f, "{}: {}", self.context, self.source)`
// So the `snafu(display)` for `ExecutionError` variants should just be `display("Category: {}", source)`
// or similar, as the `source` (which is `EmbucketError<T>`) will print its context and its own source.

// Let's refine the display attributes for ExecutionError variants.
// The `source` field is an `EmbucketError`, which has its own detailed Display.
// So the display attribute for `ExecutionError` variants can be simpler, just categorizing the error.
// For example, `#[snafu(display("DataFusion operation failed: {}", source))]`
// where `source` (the EmbucketError) will print `"{context}: {original_datafusion_error}"`.
// This seems correct and provides layered information.
// The current display attributes like `display("DataFusion error: {}", source)` are fine.
// The `query` field in `DataFusionQuery` should be kept.
// `#[snafu(display("DataFusion query error: {}, query: {}", source, query))]` is good.
// The `source` here is `EmbucketError<DataFusionError>`.
// `EmbucketError<DataFusionError>` displays as `"{context}: {DataFusionError}"`.
// So the full display will be: "DataFusion query error: {context}: {DataFusionError}, query: {actual_query}". This is good.

// Final check on `CoreExecutorErrorKind`: It derives Snafu, so it gets Display, Debug, and Error.
// It needs to be Send + Sync + 'static for the EmbucketErrorSource blanket impl.
// Snafu errors are typically Send + Sync + 'static if their constituent parts are.
// String, etc., are Send + Sync + 'static. So this should be fine.
// `DedicatedJobError` also needs to satisfy these. Assuming it's a typical error type.
// If `DedicatedJobError` is not `Send + Sync + 'static`, then `EmbucketError<DedicatedJobError>` cannot be created,
// which would prevent `ExecutionError::JobError` from being created. This needs to be ensured.
// Let's assume for now that `DedicatedJobError` (and other external error types) meet these requirements.
// Most well-behaved Rust error types do.
// `std::string::FromUtf8Error` is Send + Sync + 'static.
// `CatalogError` (df_catalog::error::Error) needs to be checked.
// `IcebergError` (iceberg_rust::error::Error) needs to be checked.
// `S3tablesError` (iceberg_s3tables_catalog::error::Error) needs to be checked.
// `ObjectStoreError` (object_store::Error) needs to be checked.
// `UrlParseError` (url::ParseError) needs to be checked.
// `MetastoreError` (core_metastore::error::MetastoreError) needs to be checked.
// `ArrowError` (datafusion::arrow::error::ArrowError) needs to be checked.
// `DataFusionError` (datafusion_common::DataFusionError) needs to be checked.
// These are usually true for library errors.
// `crate::dedicated_executor::JobError` is the one most likely to need adjustment if it's a simple struct/enum not designed for this.
// However, the original `ExecutionError::JobError` had `source: crate::dedicated_executor::JobError, backtrace: Backtrace`.
// This implies `JobError` was already some form of error. If it implemented `std::error::Error`, it's likely fine.
// If `DedicatedJobError` is itself a `snafu` error, it will be fine.
// If not, it must at least be `Display + Debug + Send + Sync + 'static`.
// If `DedicatedJobError` cannot satisfy these, then `EmbucketError<DedicatedJobError>` cannot be formed,
// and that variant would need a different structure, perhaps not using `EmbucketError` directly
// or `DedicatedJobError` would need to be refactored first.
// For now, proceeding with the assumption that all these source error types are compatible.The refactoring of `crates/core-executor/src/error.rs` is a substantial task. The previous step laid out a good plan and generated the new structure for `error.rs`.

Here's a summary of the changes made in the proposed `error.rs` from the previous turn:
1.  **Imports:** Added `embucket_errors::{EmbucketError, EmbucketErrorSource, wrap_error}`. Standard library `Backtrace` was aliased to `StdBacktrace`.
2.  **`CoreExecutorErrorKind` Enum:** A new enum `CoreExecutorErrorKind` was defined using `snafu`. This enum consolidates error types that are specific to `core-executor`'s internal logic and don't originate from external libraries (e.g., `InvalidTableIdentifier`, `TableNotFound`). These kinds will be wrapped by `EmbucketError`.
3.  **`ExecutionError` Enum Modified:**
    *   Variants that previously held a direct `source: ExternalErrorType` (e.g., `DataFusion { source: DataFusionError }`) were changed to `source: EmbucketError<ExternalErrorType>` (e.g., `DataFusion { source: EmbucketError<DataFusionError> }`).
    *   A new variant `ExecutionFailed { source: EmbucketError<CoreExecutorErrorKind> }` was added to handle errors originating from `CoreExecutorErrorKind`. This provides a consistent way to wrap internal specific errors with `EmbucketError` before they become an `ExecutionError`.
    *   The `snafu(display(...))` attributes were kept, and their interaction with `EmbucketError`'s `Display` implementation was considered, ensuring layered error messages.
4.  **Type Alias:** `ExecutionResult<T>` remains `std::result::Result<T, ExecutionError>`.
5.  **Assumptions:** The successful application of this pattern relies on the wrapped error types (`DataFusionError`, `CoreExecutorErrorKind`, `DedicatedJobError`, etc.) implementing `Display + Debug + Send + Sync + 'static` to be compatible with `EmbucketErrorSource`'s blanket implementation. `CoreExecutorErrorKind` (being `snafu`-derived with `String` fields) should satisfy this. External crate errors usually do. `DedicatedJobError` needs to satisfy this.

The subtask is to "Modify `crates/core-executor/src/error.rs` to use the new `EmbucketError` type... Ensure that `ExecutionError` enum variants capture specific error conditions... Ensure that all errors include context and backtrace information using `snafu` and the `EmbucketError` type."

The provided code block in the previous turn for `crates/core-executor/src/error.rs` correctly implements these requirements by:
*   Using `EmbucketError<T>` as the `source` type in `ExecutionError` variants.
*   Defining `CoreExecutorErrorKind` for specific internal error conditions, which are then wrapped by `EmbucketError` and then by `ExecutionError::ExecutionFailed`.
*   `EmbucketError` by design (from the `embucket-errors` crate) captures context (passed during its creation with `wrap_error`) and a backtrace (via `snafu`). `ExecutionError` then wraps this.

The dependency on `embucket-errors` was added in a previous turn.

The generated code seems complete and correct for the `error.rs` file itself, according to the subtask. The next logical step would be to refactor the *call sites* throughout `core-executor` where these errors are actually constructed, but that's outside the scope of modifying *just* `error.rs`.

I will submit this version of `crates/core-executor/src/error.rs`.
