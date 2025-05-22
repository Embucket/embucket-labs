# Error Handling in Embucket

This document outlines the philosophy and mechanics of error handling within the Embucket project. Our strategy aims to provide clear, contextual, and debuggable errors, while distinguishing between the needs of library crates and user-facing applications.

## Philosophy

- **Clear Context:** Errors should provide enough information to understand what operation was being attempted and why it failed. Context should be added as errors propagate up the call stack.
- **Backtraces:** All significant errors should capture a backtrace at the point where they are distinctively handled or generated, allowing developers to trace the origin of the problem.
- **Structured Errors:** Errors are defined as enums, allowing for programmatic inspection and differentiated handling based on error kind.
- **Library vs. Application:**
    - **Library Crates** (e.g., `core-metastore`, `core-executor`) should focus on returning detailed, structured errors. They are responsible for identifying specific failure conditions within their domain.
    - **Application/Service Crates** (e.g., `api-iceberg-rest`, `embucketd`) are responsible for translating library errors (and their own internal errors) into user-facing messages, HTTP responses, or CLI outputs. They also handle the primary logging of full error details including backtraces.

## Core Components

### 1. `embucket-errors` Crate

This crate is the cornerstone of our error handling strategy.

-   **`EmbucketErrorSource` Trait:**
    -   Defined as: `pub trait EmbucketErrorSource: Display + Debug + Send + Sync + 'static {}`
    -   A marker trait indicating that an error type can be wrapped by `EmbucketError<T>`.
    -   A blanket implementation is provided: `impl<T> EmbucketErrorSource for T where T: Display + Debug + Send + Sync + 'static {}`. This means any error type satisfying these bounds (common for most Rust errors) can be a source.

-   **`EmbucketError<T: EmbucketErrorSource>` Struct:**
    -   The primary wrapper for adding context and backtraces.
    -   Structure:
        ```rust
        pub struct EmbucketError<T: EmbucketErrorSource> {
            pub source: T,      // The original error being wrapped
            pub context: String,  // Additional context provided at the wrapping site
            backtrace: Backtrace, // Captured by snafu when EmbucketError is created
        }
        ```
    -   **Display Format:** `EmbucketError<T>` implements `Display` as `"{context}: {source_error_string}"`. This creates a chain of contexts as errors are wrapped.
    -   **Backtrace Access:** Provides a `get_backtrace() -> &Backtrace` method.

-   **`wrap_error<T: EmbucketErrorSource>(source: T, context: String) -> EmbucketError<T>` Function:**
    -   The **primary way to create an instance of `EmbucketError<T>`**.
    -   It takes the `source` error and a `context` string, returning the wrapped error.
    -   `EmbucketError<T>` derives `snafu::Snafu`, so `wrap_error` is essentially a constructor that ensures a `Backtrace` is captured at this point.

### 2. `snafu` Crate Integration

We use `snafu` for defining structured error enums within each crate.

-   **Typical Structure:** Most crates will define two enums:
    1.  **`SpecificErrorKind` (e.g., `MetastoreErrorKind`, `CliErrorKind`):**
        -   Defines specific error conditions unique to that crate's internal logic.
        -   Variants usually contain fields providing details about the error.
        -   Example:
            ```rust
            #[derive(Debug, Snafu)]
            #[snafu(visibility(pub(crate)))]
            pub enum MyCrateErrorKind {
                #[snafu(display("Configuration item '{key}' not found"))]
                ConfigNotFound { key: String },
                // ... other specific kinds
            }
            ```
    2.  **Main `Error` Enum (e.g., `MetastoreError`, `CliError`, `IcebergAPIError`):**
        -   The primary error type returned by functions in the crate.
        -   Its variants wrap either:
            -   An `EmbucketError<ExternalErrorType>`: For errors originating from external libraries (e.g., `std::io::Error`, `serde_json::Error`, another Embucket crate's `Error` type).
            -   An `EmbucketError<OwnErrorKind>`: For errors defined in the crate's own `SpecificErrorKind`.
        -   Example:
            ```rust
            #[derive(Debug, Snafu)]
            #[snafu(visibility(pub(crate)))]
            pub enum MyCrateError {
                #[snafu(display("I/O operation failed: {}", source))]
                Io { source: EmbucketError<std::io::Error> },

                #[snafu(display("My crate specific failure: {}", source))]
                OperationFailed { source: EmbucketError<MyCrateErrorKind> },
            }
            ```

-   **Context Selectors:** `snafu` generates context selector structs (e.g., `IoSnafu`, `OperationFailedSnafu`) that are used to construct instances of the main `Error` enum.

## Error Handling Patterns by Crate Type

### Library Crates (e.g., `core-metastore`, `core-executor`)

1.  **Define `YourLibraryErrorKind` and `YourLibraryError`** as described above.
2.  **Error Generation:**
    -   When a specific internal condition fails:
        ```rust
        // 1. Create the SpecificErrorKind
        let kind_error = MyLibraryErrorKind::SomethingSpecificFailed { detail: "foo".to_string() };
        // 2. Wrap it with EmbucketError, providing context about the operation
        let embucket_err = wrap_error(kind_error, "While performing high-level operation X".to_string());
        // 3. Construct and return YourLibraryError using snafu context selector
        return OperationFailedSnafu { source: embucket_err }.fail();
        ```
    -   When an error from an external dependency (e.g., `std::io::Error`) occurs:
        ```rust
        // 1. The external error (e.g., io_error)
        // 2. Wrap it with EmbucketError, providing context
        let embucket_io_err = wrap_error(io_error, format!("Failed to read file '{}'", path));
        // 3. Construct and return YourLibraryError using snafu context selector
        return IoSnafu { source: embucket_io_err }.fail();
        ```
    -   When an error from another Embucket library function is received:
        ```rust
        // other_lib_result: Result<_, OtherLibError>
        other_lib_result.map_err(|other_lib_error| {
            // 1. other_lib_error already contains an EmbucketError
            // 2. Wrap it again with *this library's* EmbucketError, adding this library's context
            let embucket_wrapped_other_lib_error = wrap_error(
                other_lib_error, // This is OtherLibError, which is EmbucketErrorSource
                "Context from current library operation".to_string()
            );
            // 3. Construct this library's error variant that is designated for OtherLibError
            //    (e.g., OtherLibSnafu { source: embucket_wrapped_other_lib_error }).
            //    Or, if OtherLibError is treated as a generic external error:
            ExternalFailureSnafu { source: embucket_wrapped_other_lib_error }.build()
        })?;
        ```
        Alternatively, if `OtherLibError` is a direct `source` for a variant in `MyLibraryError`:
        ```rust
        // MyLibraryError has: OtherLibVariant { source: EmbucketError<OtherLibError> }
        other_lib_result.context(OtherLibSnafu { context: "Context from current library".to_string() })?;
        // This requires OtherLibSnafu to accept a context field that it passes to wrap_error internally,
        // or for the map_err approach to be used. The map_err approach is more explicit about the wrap_error call.
        // Our current pattern is more like the map_err approach:
        // other_lib_result.map_err(|e| OtherLibSnafu { source: wrap_error(e, "context") }.build())?;
        ```
        The most common pattern for chaining errors from other Embucket libraries:
        ```rust
        // Current crate: MyCrate, calling function in OtherCrate which returns Result<_, OtherError>
        // MyCrateError has a variant:
        // OtherCrateFailed { source: EmbucketError<OtherError> }

        let result_from_other_crate = other_crate::some_function().map_err(|other_err: OtherError| {
            let wrapped_err = wrap_error(other_err, "Context from MyCrate about why other_crate::some_function was called".to_string());
            OtherCrateFailedSnafu { source: wrapped_err }.build()
        })?;
        ```

3.  **Goal:** Propagate errors upwards with added contextual information at each step where the error is handled or re-interpreted.

### Application/Service Crates (e.g., `api-iceberg-rest`, `embucketd`)

1.  **Define `YourAppErrorKind` and `YourAppError`** similar to libraries.
2.  **Handling Library Errors:**
    -   When calling a function from an Embucket library (e.g., `core-metastore`):
        ```rust
        // result_from_metastore: Result<_, MetastoreError>
        let data = result_from_metastore.map_err(|metastore_error| {
            let wrapped_metastore_err = wrap_error(
                metastore_error, // This is MetastoreError, which is EmbucketErrorSource
                "API context: Failed to retrieve entity X while processing Y request".to_string()
            );
            // Construct this application's error variant for MetastoreError
            MyApiError::MetastoreOperation { source: wrapped_metastore_err } // Or using Snafu: MetastoreOpSnafu { source: wrapped_metastore_err }.build()
        })?;
        ```
3.  **Logging:**
    -   These crates are responsible for the primary logging of the full error details.
    -   Typically done at the point where the error can no longer be propagated (e.g., in `IntoResponse` for APIs, or in `main()` for CLIs).
    -   Log the full error string (`error.to_string()`) which includes the chain of contexts.
    -   Log the backtrace obtained from the relevant `EmbucketError` (e.g., `innermost_embucket_error.get_backtrace()`).
    -   Example (conceptual):
        ```rust
        // In api-iceberg-rest's IntoResponse for IcebergAPIError:
        tracing::error!(
            error.message = %self.to_string(), // Full chained message
            error.backtrace = %self.get_innermost_backtrace_string(), // Helper to get relevant backtrace
            "API request failed"
        );

        // In embucketd's main:
        if let Err(e) = run_app() {
            tracing::error!(
                error.message = %e.to_string(),
                error.backtrace = %e.get_innermost_backtrace_string(), // Helper
                "Application error"
            );
        }
        ```

4.  **User-Facing Output:**
    -   **`api-iceberg-rest`:** The `IntoResponse` implementation should transform the `IcebergAPIError` into a user-friendly JSON response, including an appropriate HTTP status code. It should *not* expose raw backtraces in production responses by default.
    -   **`embucketd`:** The `main` function should print a user-friendly error message to `stderr`. Raw backtraces should only be printed if a debug flag (e.g., `RUST_BACKTRACE=1` or `EMBUCKET_DEBUG=1`) is enabled.

## Adding or Modifying Errors

1.  **Identify the Crate:** Determine which crate the error originates in or is most relevant to.
2.  **ErrorKind or External?**
    -   If it's a new, specific failure condition within that crate's logic, add a variant to its `SpecificErrorKind` enum.
    -   If it's an error from an external dependency (or another Embucket crate) that needs to be handled, ensure the main `Error` enum has a variant to wrap an `EmbucketError<ThatErrorType>`.
3.  **Update Error Creation Sites:**
    -   Use `wrap_error("original_error_or_kind", "contextual_message".to_string())` to create the `EmbucketError`.
    -   Use the appropriate `snafu` context selector (e.g., `VariantNameSnafu { source: wrapped_embucket_error }.fail()`) to construct and return the main `Error` enum instance.
4.  **Add Context:** Ensure the `context` string provided to `wrap_error` is meaningful and describes what the code was trying to achieve when the error occurred.
5.  **Application Layer Update (if applicable):**
    -   If the error is from a library, the application layer might need to update its own error handling to specifically recognize and map this new library error, potentially to a specific HTTP status code or CLI message.
6.  **Logging & Testing:**
    -   Ensure top-level applications log the error appropriately.
    -   Add tests for the new error condition, checking for correct wrapping, context, and propagation.

## Backtraces

-   Backtraces are captured when an `EmbucketError<T>` is created (implicitly by `snafu` which `EmbucketError<T>` derives).
-   They are primarily for developer debugging.
-   Access them via the `get_backtrace()` method on `EmbucketError`.
-   User-facing applications (`api-iceberg-rest`, `embucketd`) should log them server-side or make them available under debug flags, but not expose them directly to users in release builds by default.
-   Enable backtraces during development by setting `RUST_BACKTRACE=1` or `EMBUCKET_DEBUG=1` (for `embucketd`'s specific handling). `tracing` setup might also be influenced by `RUST_LOG`.

This document provides a guideline. Specific implementations may vary slightly, but the core principles of using `embucket-errors` for context/backtrace and `snafu` for structured error enums should be consistent.
