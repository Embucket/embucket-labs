pub mod entities;
pub mod errors;
pub mod interface;

cfg_if::cfg_if! {
    if #[cfg(feature = "sqlite")]
    {
        pub mod sqlite_history_store;
        pub use sqlite_history_store::*;
    } else {
        pub mod slatedb_history_store;
        pub use slatedb_history_store::*;
    }
}

#[cfg(test)]
pub mod tests;

pub use entities::*;
pub use errors::*;
pub use interface::*;

#[cfg(test)]
pub use interface::MockHistoryStore;
