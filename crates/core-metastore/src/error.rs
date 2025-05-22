use snafu::prelude::*;

// Import from embucket-errors
use embucket_errors::{EmbucketError, EmbucketErrorSource, wrap_error};

// Original error sources from other crates
use iceberg_rust_spec::table_metadata::TableMetadataBuilderError;
use object_store::Error as ObjectStoreError;
use object_store::path::Error as ObjectStorePathError;
use std::io::Error as StdIoError;
use slatedb::SlateDBError;
use core_utils::Error as CoreUtilsError; // Assuming this is an error type from core-utils
use iceberg_rust::error::Error as IcebergError;
use serde_json::Error as SerdeJsonError;
use validator::ValidationErrors;
use url::ParseError as UrlParseError;

// --- Define Metastore's own specific error kinds ---
// These will be wrapped by EmbucketError and then by a variant in MetastoreError
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MetastoreErrorKind {
    #[snafu(display("Table data already exists at that location: {location}"))]
    TableDataExists { location: String },

    #[snafu(display("Table requirement failed: {message}"))]
    TableRequirementFailed { message: String },

    #[snafu(display("Volume: Validation failed. Reason: {reason}"))]
    VolumeValidationFailed { reason: String },

    #[snafu(display("Volume: Missing credentials"))]
    VolumeMissingCredentials,

    #[snafu(display("Cloud provider not implemented: {provider}"))]
    CloudProviderNotImplemented { provider: String },

    #[snafu(display("Metastore object of type {type_name} with name {name} already exists"))]
    ObjectAlreadyExists { type_name: String, name: String },

    #[snafu(display("Metastore object not found"))]
    ObjectNotFound,

    #[snafu(display("Volume {volume} already exists"))]
    VolumeAlreadyExists { volume: String },

    #[snafu(display("Volume {volume} not found"))]
    VolumeNotFound { volume: String },

    #[snafu(display("Database {db} already exists"))]
    DatabaseAlreadyExists { db: String },

    #[snafu(display("Database {db} not found"))]
    DatabaseNotFound { db: String },

    #[snafu(display("Schema {schema} already exists in database {db}"))]
    SchemaAlreadyExists { schema: String, db: String },

    #[snafu(display("Schema {schema} not found in database {db}"))]
    SchemaNotFound { schema: String, db: String },

    #[snafu(display("Table {table} already exists in schema {schema} in database {db}"))]
    TableAlreadyExists {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display("Table {table} not found in schema {schema} in database {db}"))]
    TableNotFound {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display(
        "Table Object Store for table {table} in schema {schema} in database {db} not found"
    ))]
    TableObjectStoreNotFound {
        table: String,
        schema: String,
        db: String,
    },

    #[snafu(display("Volume in use by database(s): {database}"))]
    VolumeInUse { database: String },

    // This will wrap validator::ValidationErrors
    #[snafu(display("Input validation failed: {source}"))]
    Validation { source: ValidationErrors },
}

// --- Main MetastoreError Enum ---
// Each variant will typically hold an EmbucketError wrapping a more specific error
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))] // Changed visibility to pub(crate) for consistency, can be pub if needed by external users
pub enum MetastoreError {
    #[snafu(display("ObjectStore operation failed: {}", source))]
    ObjectStore { source: EmbucketError<ObjectStoreError> },

    #[snafu(display("ObjectStore path error: {}", source))]
    ObjectStorePath { source: EmbucketError<ObjectStorePathError> },

    #[snafu(display("Unable to create directory for File ObjectStore path {path}: {}", source))]
    CreateDirectory {
        path: String, // Keep path for specific context here
        source: EmbucketError<StdIoError>,
    },

    #[snafu(display("SlateDB operation failed: {}", source))]
    SlateDB { source: EmbucketError<SlateDBError> },

    #[snafu(display("Core Utils error: {}", source))] // Assuming CoreUtilsError is a compatible error type
    UtilSlateDB { source: EmbucketError<CoreUtilsError> },

    #[snafu(display("Iceberg operation failed: {}", source))]
    Iceberg { source: EmbucketError<IcebergError> },

    #[snafu(display("Iceberg TableMetadataBuilder failed: {}", source))]
    TableMetadataBuilder { source: EmbucketError<TableMetadataBuilderError> },

    #[snafu(display("Serialization/Deserialization error: {}", source))]
    Serde { source: EmbucketError<SerdeJsonError> },

    #[snafu(display("URL parsing error: {}", source))]
    UrlParse { source: EmbucketError<UrlParseError> },

    // This variant wraps MetastoreErrorKind for errors originating within this crate's logic
    #[snafu(display("Metastore operation failed: {}", source))]
    OperationFailed { source: EmbucketError<MetastoreErrorKind> },
}

pub type MetastoreResult<T> = std::result::Result<T, MetastoreError>;

// Updated From implementation for ValidationErrors
// Now, it will be wrapped into MetastoreErrorKind first, then EmbucketError, then MetastoreError::OperationFailed
// This requires call sites to change how they handle ValidationErrors.
// Previously: `Err(error.into())`
// Now:
// ```
// result.map_err(|validation_errors| {
//     let kind = MetastoreErrorKind::Validation { source: validation_errors };
//     let wrapped_error = wrap_error(kind, "Entity validation failed".to_string());
//     MetastoreError::OperationFailed { source: wrapped_error }
// })
// ```
// Or more simply, using a context selector:
// `result.map_err(|e| MetastoreErrorKind::Validation { source: e })
//        .context(OperationFailedSnafu { context: "Entity validation failed".to_string() })?`
// This is more involved. A direct `From` might be too simplistic if we want to add context via `wrap_error`.

// For simplicity in the error.rs file, we might not provide a direct From anymore,
// or if we do, it would create a default context.
// Let's remove the `From` implementation for now. Users will need to explicitly
// wrap `ValidationErrors` using `MetastoreErrorKind::Validation`, then `wrap_error`,
// then `MetastoreError::OperationFailedSnafu`.
// This ensures context is consciously added.

// Example of how errors would be constructed (conceptual, for use in other modules):
//
// fn example_usage_internal_error() -> MetastoreResult<()> {
//     let db_name = "test_db";
//     let kind_error = MetastoreErrorKind::DatabaseNotFound { db: db_name.to_string() };
//     let embucket_err = wrap_error(kind_error, format!("Attempting to access database '{}'", db_name));
//     return OperationFailedSnafu { source: embucket_err }.fail();
// }
//
// fn example_usage_external_error(some_path: &str) -> MetastoreResult<()> {
//     match std::fs::read_to_string(some_path) {
//         Ok(content) => Ok(()), // process content
//         Err(io_error) => {
//             let embucket_err = wrap_error(io_error, format!("Failed to read file at '{}'", some_path));
//             // If there was a MetastoreError variant for StdIoError directly:
//             // return IoErrorSnafu { source: embucket_err }.fail();
//             // Since we have CreateDirectory for StdIoError, let's use that as an example if it fits.
//             // If it's a generic IO error, we might need a more generic MetastoreError variant or map it
//             // to a MetastoreErrorKind.
//             // For this example, let's assume we add a generic IO error variant to MetastoreError for illustration:
//             // (Illustrative - this variant is not in the main MetastoreError above)
//             // #[snafu(display("I/O error: {}", source))]
//             // IoError { source: EmbucketError<StdIoError> },
//             //
//             // return IoErrorSnafu { source: embucket_err }.fail();
//
//             // Using the current structure:
//             // If the std::io::Error was from creating a directory:
//             return CreateDirectorySnafu { path: some_path.to_string(), source: embucket_err }.fail();
//         }
//     }
// }

// All error types used as T in EmbucketError<T> (e.g., ObjectStoreError, SlateDBError, MetastoreErrorKind)
// must implement Display + Debug + Send + Sync + 'static for the blanket implementation of EmbucketErrorSource.
// MetastoreErrorKind, being snafu-derived with String fields and ValidationErrors (which is Send+Sync), should be fine.
// External crate error types are generally expected to satisfy these.
// TableMetadataBuilderError from iceberg_rust_spec needs to be Send + Sync + 'static.
// core_utils::Error needs to be Send + Sync + 'static.
// If any of these don't satisfy the bounds, `EmbucketError` cannot wrap them.
// This is a crucial part of the integration.
