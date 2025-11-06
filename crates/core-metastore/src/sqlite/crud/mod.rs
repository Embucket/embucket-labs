pub mod table;
pub mod volumes;
pub mod databases;
pub mod schemas;

use chrono::Utc;

#[must_use]
pub fn current_ts_str() -> String {
    Utc::now().to_rfc3339()
}