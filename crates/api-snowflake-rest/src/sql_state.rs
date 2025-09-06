use serde::{Deserialize, Serialize};
use std::fmt::Display;

// See also ErrorCode, which is also returned by this transport
// crates/core-executor/src/error_code.rs

// Kept fo reference from ANSI standard:
// SQLSTATE - consists of 5 bytes. They are divided into two parts: the first and second bytes contain a class
// and the following three a subclass.
// Each class belongs to one of four categories:
// "S" denotes "Success" (class 00),
// "W" denotes "Warning" (class 01),
// "N" denotes "No data" (class 02),
// "X" denotes "Exception" (all other classes).

// Just mimic snowflake's SQLSTATE, as it looks not much relevant to ANSI standard
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SqlState {
    #[serde(rename = "02000")]
    Success,
    // Snowflake return such sqlstate for syntax error
    #[serde(rename = "42000")]
    SyntaxError,
    #[serde(rename = "42501")]
    CantLocateQueryResult,
    #[serde(rename = "42502")]
    DoesNotExist,
    // Following code returned from every errored query result loaded from history
    // As currently we don't save SqlState when save result to history
    #[serde(rename = "42503")]
    GenericQueryErrorFromHistory,
}

impl Display for SqlState {
    #[allow(clippy::as_conversions)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // check if serializable value has starting, traling double quotes
        let serialized = serde_json::to_string(self).unwrap_or_default();
        if serialized.starts_with('"') && serialized.ends_with('"') {
            // fetch value between double quotes
            let value = &serialized[1..serialized.len() - 1];
            let parsed = value.parse::<u32>();
            if let Ok(parsed) = parsed {
                return write!(f, "{parsed:05}");
            }
        }
        // serde serialized just name of enum variant, get number instead
        write!(f, "{:05}", *self as u32)
    }
}
