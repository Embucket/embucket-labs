pub mod config;
pub mod error;
pub mod handler;
pub mod router;
#[cfg(test)]
pub mod tests;

pub use error::Error;
pub use router::web_assets_app;
