pub mod entities;
pub mod errors;
pub mod interface;
pub mod store;

cfg_if::cfg_if! {
    if #[cfg(feature = "sqlite")]
    {
        pub mod sqlite_history_store;
    } else {
        pub mod slatedb_history_store;
    }
}

#[cfg(test)]
pub mod tests;

pub use entities::*;
pub use errors::*;
pub use interface::*;
pub use store::*;

#[cfg(test)]
pub use interface::MockHistoryStore;
