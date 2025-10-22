pub use df_catalog as catalog;
pub mod datafusion;
pub mod dedicated_executor;
pub mod duckdb;
pub mod error;
pub mod error_code;
pub mod models;
pub mod query;
pub mod running_queries;
pub mod service;
pub mod session;
pub mod snowflake_error;
pub mod tracing;
pub mod utils;

#[cfg(test)]
pub mod tests;

pub use error::{Error, Result};
pub use running_queries::RunningQueryId;
pub use snowflake_error::SnowflakeError;

use crate::service::ExecutionService;
use std::sync::Arc;

pub trait ExecutionAppState {
    fn get_execution_svc(&self) -> Arc<dyn ExecutionService>;
}
