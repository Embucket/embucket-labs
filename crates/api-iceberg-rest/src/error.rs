use axum::{response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tracing; // Added tracing

// Import from embucket-errors
use embucket_errors::{EmbucketError, EmbucketErrorSource, wrap_error};

// Errors from other crates that IcebergAPIError might wrap
use core_metastore::error::MetastoreError;
use iceberg_rust::error::Error as IcebergRustError;
use serde_json::Error as SerdeJsonError;
use validator::ValidationErrors;
// If core_executor::error::ExecutionError is used, it should be imported too.

// --- Define API Layer's own specific error kinds ---
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ApiIcebergRestErrorKind {
    #[snafu(display("Invalid request parameter: {name}, reason: {reason}"))]
    InvalidRequestParameter { name: String, reason: String },

    #[snafu(display("Failed to parse namespace: {namespace_str}"))]
    NamespaceParsingError { namespace_str: String },

    #[snafu(display("An unexpected error occurred while processing the request."))]
    UnexpectedProcessingError,
    // Add other specific API error kinds here
}

// --- Main IcebergAPIError Enum ---
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum IcebergAPIError {
    #[snafu(display("Metastore operation failed: {}", source))]
    MetastoreOperation { source: EmbucketError<MetastoreError> },

    #[snafu(display("Iceberg library error: {}", source))]
    IcebergLibFailure { source: EmbucketError<IcebergRustError> },

    #[snafu(display("JSON serialization/deserialization error: {}", source))]
    Serialization { source: EmbucketError<SerdeJsonError> },

    #[snafu(display("Input validation failed: {}", source))]
    Validation { source: EmbucketError<ValidationErrors> },
    
    #[snafu(display("API request processing error: {}", source))]
    RequestProcessingFailed { source: EmbucketError<ApiIcebergRestErrorKind> },

    #[snafu(display("Storage interaction error: {}", source))]
    ObjectStoreInteraction { source: EmbucketError<object_store::Error> },
}

impl IcebergAPIError {
    // Helper to get the backtrace from the wrapped EmbucketError
    fn get_embucket_backtrace(&self) -> Option<String> {
        let bt_opt = match self {
            IcebergAPIError::MetastoreOperation { source } => Some(source.get_backtrace()),
            IcebergAPIError::IcebergLibFailure { source } => Some(source.get_backtrace()),
            IcebergAPIError::Serialization { source } => Some(source.get_backtrace()),
            IcebergAPIError::Validation { source } => Some(source.get_backtrace()),
            IcebergAPIError::RequestProcessingFailed { source } => Some(source.get_backtrace()),
            IcebergAPIError::ObjectStoreInteraction { source } => Some(source.get_backtrace()),
        };
        bt_opt.map(|bt| bt.to_string())
    }
}

pub type IcebergAPIResult<T> = Result<T, IcebergAPIError>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>, // Changed to Option<String> for direct backtrace string
}


impl IntoResponse for IcebergAPIError {
    fn into_response(self) -> axum::response::Response {
        let error_for_logging = format!("{}", self); // Full error string for logging
        let backtrace_for_logging = self.get_embucket_backtrace().unwrap_or_else(|| "No backtrace available".to_string());

        // Server-side logging
        tracing::error!(
            error_type = %std::any::type_name::<Self>(), // Consider a more specific type string if needed
            message = %error_for_logging,
            backtrace = %backtrace_for_logging,
            "API error occurred"
        );
        
        let error_message_for_client = self.to_string(); // Or a more sanitized version if needed
        
        let (status_code, error_type_str) = match &self {
            IcebergAPIError::MetastoreOperation { source } => {
                let metastore_err = &source.source;
                let code = match metastore_err {
                    MetastoreError::OperationFailed { source: embucket_metastore_kind } => {
                        match &embucket_metastore_kind.source {
                            core_metastore::error::MetastoreErrorKind::TableDataExists { .. }
                            | core_metastore::error::MetastoreErrorKind::ObjectAlreadyExists { .. }
                            | core_metastore::error::MetastoreErrorKind::VolumeAlreadyExists { .. }
                            | core_metastore::error::MetastoreErrorKind::DatabaseAlreadyExists { .. }
                            | core_metastore::error::MetastoreErrorKind::SchemaAlreadyExists { .. }
                            | core_metastore::error::MetastoreErrorKind::TableAlreadyExists { .. }
                            | core_metastore::error::MetastoreErrorKind::VolumeInUse { .. } => StatusCode::CONFLICT,
                            core_metastore::error::MetastoreErrorKind::TableRequirementFailed { .. } => StatusCode::UNPROCESSABLE_ENTITY,
                            core_metastore::error::MetastoreErrorKind::VolumeValidationFailed { .. }
                            | core_metastore::error::MetastoreErrorKind::VolumeMissingCredentials => StatusCode::BAD_REQUEST,
                            core_metastore::error::MetastoreErrorKind::Validation { .. } => StatusCode::BAD_REQUEST,
                            core_metastore::error::MetastoreErrorKind::CloudProviderNotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
                            core_metastore::error::MetastoreErrorKind::VolumeNotFound { .. }
                            | core_metastore::error::MetastoreErrorKind::DatabaseNotFound { .. }
                            | core_metastore::error::MetastoreErrorKind::SchemaNotFound { .. }
                            | core_metastore::error::MetastoreErrorKind::TableNotFound { .. }
                            | core_metastore::error::MetastoreErrorKind::TableObjectStoreNotFound { .. }
                            | core_metastore::error::MetastoreErrorKind::ObjectNotFound => StatusCode::NOT_FOUND,
                            _ => StatusCode::INTERNAL_SERVER_ERROR,
                        }
                    }
                    MetastoreError::Iceberg { .. } 
                    | MetastoreError::ObjectStore { .. } 
                    | MetastoreError::Serde { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (code, "MetastoreOperationError")
            }
            IcebergAPIError::IcebergLibFailure { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "IcebergLibraryError"),
            IcebergAPIError::Serialization { .. } => (StatusCode::BAD_REQUEST, "SerializationError"),
            IcebergAPIError::Validation { .. } => (StatusCode::BAD_REQUEST, "ValidationError"),
            IcebergAPIError::RequestProcessingFailed { source } => {
                let code = match &source.source {
                    ApiIcebergRestErrorKind::InvalidRequestParameter { .. } => StatusCode::BAD_REQUEST,
                    ApiIcebergRestErrorKind::NamespaceParsingError { .. } => StatusCode::BAD_REQUEST,
                    ApiIcebergRestErrorKind::UnexpectedProcessingError => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (code, "RequestProcessingError")
            }
            IcebergAPIError::ObjectStoreInteraction { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "StorageInteractionError"),
        };

        // Decide if stack trace is included in client response (e.g., based on env var or debug mode)
        // For simplicity, let's assume RUST_BACKTRACE or a similar flag controls this for client response.
        // Server log always gets it.
        let stack_for_client = if std::env::var("RUST_BACKTRACE").map_or(false, |val| val == "1" || val == "full") ||
                                  std::env::var("EMBUCKET_DEBUG").map_or(false, |val| val == "1" || val.eq_ignore_ascii_case("true")) {
            Some(backtrace_for_logging)
        } else {
            None
        };

        let error_response = ErrorResponse {
            error_type: error_type_str.to_string(),
            message: error_message_for_client, // Use the full error string for client, or simplify it.
                                               // self.to_string() is `"{api_context}: {lower_layer_error_string}"`
                                               // This might be okay for now.
            stack: stack_for_client,
        };

        (status_code, Json(error_response)).into_response()
    }
}
