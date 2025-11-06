pub mod error;
pub mod interface;
pub mod list_parameters;
pub mod models;

cfg_if::cfg_if! {
    if #[cfg(feature = "sqlite")]
    {
        pub mod sqlite;
        pub mod sqlite_metastore;
        pub use sqlite_metastore::*;
    } else {
        pub mod metastore;
        pub use metastore::*;
    }
}

#[cfg(test)]
pub mod tests;

pub use error::{Error, Result};
pub use interface::*;
pub use list_parameters::*;
pub use models::*;
