// Put QueryIdParam here to avoid duplicates 
// This is used by rest apis

use crate::QueryRecordId;
use serde::{Deserialize, Deserializer};
use std::str::FromStr;
use uuid::Uuid;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum QueryIdError {
    #[snafu(display("invalid query id integer: {query_id}"))]
    InvalidQueryIdInt {
        query_id: String,
    },
    #[snafu(display("invalid query id uuid : {query_id}"))]
    InvalidQueryIdUuid {
        query_id: String,
    },
    #[snafu(display("expected integer or uuid, got: {query_id}"))]
    InvalidQueryIdType {
        query_id: String,
    },
}


#[derive(Debug, Clone)]
pub enum QueryIdParam {
    Int(i64),
    Uuid(uuid::Uuid),
}

impl<'de> Deserialize<'de> for QueryIdParam {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = serde_json::Value::deserialize(deserializer)?;
        let query_id = val.to_string();
        match val {
            serde_json::Value::Number(n) => {
                n.as_i64()
                    .map(Self::Int)
                    .ok_or_else(|| serde::de::Error::custom(InvalidQueryIdIntSnafu { query_id }.build() ))
            }
            serde_json::Value::String(s) => {
                // check if string contains "-" to determine if it is a uuid
                if s.contains("-") {
                    Uuid::parse_str(&s)
                        .map(Self::Uuid)
                        .map_err(|_| serde::de::Error::custom(InvalidQueryIdUuidSnafu { query_id }.build()))
                } else {
                    // i64 from string
                    i64::from_str(&s)
                        .map(Self::Int)
                        .map_err(|_| serde::de::Error::custom(InvalidQueryIdIntSnafu { query_id }.build()))
                }
            }
            _ => Err(serde::de::Error::custom(InvalidQueryIdTypeSnafu { query_id }.build())),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<QueryRecordId> for QueryIdParam {
    fn into(self) -> QueryRecordId {
        match self {
            Self::Int(i64) => QueryRecordId::from(i64),
            Self::Uuid(uuid) => QueryRecordId::from(uuid),
        }
    }
}