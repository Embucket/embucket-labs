// pub mod error; // Removed as error.rs will be deleted
pub mod metastore;
// pub mod models; // Removed as models/ directory will be deleted

pub use metastore::SlateDBMetastore; // Explicitly export SlateDBMetastore
// Metastore trait and models are now in core-traits
