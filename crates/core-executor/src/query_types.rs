use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct QueryRecordId(pub i64);

impl QueryRecordId {
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn as_uuid(self) -> Uuid {
        let mut bytes = [0u8; 16];
        bytes[8..].copy_from_slice(&self.0.to_be_bytes());
        Uuid::from_bytes(bytes)
    }
}

impl Debug for QueryRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for QueryRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for QueryRecordId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<QueryRecordId> for i64 {
    fn from(value: QueryRecordId) -> Self {
        value.0
    }
}

impl FromStr for QueryRecordId {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<Uuid> for QueryRecordId {
    fn from(value: Uuid) -> Self {
        Self(i64::from_be_bytes(
            value.as_bytes()[8..16].try_into().unwrap(),
        ))
    }
}

impl From<QueryRecordId> for Uuid {
    fn from(value: QueryRecordId) -> Self {
        value.as_uuid()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryStatus {
    Running,
    Successful,
    Failed,
    Canceled,
    TimedOut,
}
