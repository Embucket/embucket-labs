use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::{fmt::{Debug, Display}, str::FromStr};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryRecordId(pub i64);

impl QueryRecordId {
    pub fn as_i64(&self) -> i64 {
        self.0
    }

    pub fn to_uuid(self) -> Uuid {
        self.into()
    }
}

impl Display for QueryRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for QueryRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Into<i64> for QueryRecordId {
    fn into(self) -> i64 {
        self.0
    }
}

impl From<i64> for QueryRecordId {
    fn from(query_id: i64) -> Self {
        Self(query_id)
    }
}

impl FromStr for QueryRecordId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

// Only applied for uuids created by QueryRecordId::into
impl From<Uuid> for QueryRecordId {
    #[allow(clippy::unwrap_used)]
    fn from(uuid: Uuid) -> Self {
        Self(i64::from_be_bytes(uuid.as_bytes()[8..16].try_into().unwrap()))
    }
}

impl Into<Uuid> for QueryRecordId {
    fn into(self) -> Uuid {
        let mut bytes = [0u8; 16];
        bytes[8..].copy_from_slice(&self.0.to_be_bytes());
        Uuid::from_bytes(bytes)
    }
}