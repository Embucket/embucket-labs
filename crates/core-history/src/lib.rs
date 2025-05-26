pub mod entities;
pub mod history_store; // Contains the impl WorksheetsStore for SlateDBWorksheetsStore
pub mod recording_service;
pub mod store; // Contains SlateDBWorksheetsStore struct definition

// Explicitly export only the necessary public interface of this crate
pub use recording_service::RecordingExecutionService;
pub use store::SlateDBWorksheetsStore;
// QueryRecordReference is used internally by SlateDBWorksheetsStore and RecordingExecutionService.
// If it's not meant to be part of core-history's public API, it shouldn't be re-exported.
// For now, let's assume it's not, to keep the API surface minimal.
// If it is needed, it can be added: pub use entities::QueryRecordReference;
