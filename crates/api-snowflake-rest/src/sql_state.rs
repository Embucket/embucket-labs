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
#[derive(Debug, Clone, Copy)]
pub enum SqlState {
    Success,
    SyntaxError,
    CantLocateQueryResult,
    DoesNotExist,
    GenericQueryErrorFromHistory,
    FeatureNotSupported,
}

impl Display for SqlState {
    #[allow(clippy::as_conversions)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sql_state: &str = match self {
            Self::Success => "02000",
            Self::SyntaxError => "42000",
            Self::CantLocateQueryResult => "42S01",
            Self::DoesNotExist => "42S02",
            // Following code returned from every errored query result loaded from history
            // As currently we don't save SqlState when save result to history
            Self::GenericQueryErrorFromHistory => "42S03",
            Self::FeatureNotSupported => "0A000",
        };
        write!(f, "{sql_state:05}")
    }
}
