use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct QueryRecordId(pub i64);

impl QueryRecordId {
    #[must_use]
    pub const fn as_i64(&self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        let mut bytes = [0u8; 16];
        bytes[8..].copy_from_slice(&self.0.to_be_bytes());
        Uuid::from_bytes(bytes)
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

#[allow(clippy::from_over_into)]
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
#[allow(clippy::fallible_impl_from)]
impl From<Uuid> for QueryRecordId {
    #[allow(clippy::unwrap_used)]
    fn from(uuid: Uuid) -> Self {
        // it is safe in our case
        Self(i64::from_be_bytes(
            uuid.as_bytes()[8..16].try_into().unwrap(),
        ))
    }
}

#[allow(clippy::from_over_into)]
impl Into<Uuid> for QueryRecordId {
    fn into(self) -> Uuid {
        self.as_uuid()
    }
}
