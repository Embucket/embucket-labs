//! Shared benchmark utilities
mod options;
mod run;

use core_executor::models::QueryContext;
pub use options::CommonOpt;
pub use run::{BenchQuery, BenchmarkRun};

pub fn query_context() -> QueryContext {
    QueryContext::new(
        Some("embucket".to_string()),
        Some("benchmark".to_string()),
        None,
    )
}
