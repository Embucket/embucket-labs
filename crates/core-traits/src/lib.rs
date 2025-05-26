// This is the main library file for the core-traits crate.
// It will be populated with trait definitions in a later step.

pub mod executor;
pub use executor::*;

// Metastore module, containing traits, models, and errors related to the metastore.
pub mod metastore;
pub use metastore::*;

pub mod history;
pub use history::*;

// Placeholder modules `error`, `df_catalog_error`, and `dedicated_executor`
// are no longer needed as the types they were placeholdering in executor.rs
// have been changed to Box<dyn std::error::Error + Send + Sync + 'static>
// or correctly repathed (like MetastoreError).
