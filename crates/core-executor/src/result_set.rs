use bytes::Bytes;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::fmt;

use crate::error as ex_error;
use crate::query_types::QueryRecordId;

pub const QUERY_HISTORY_HARD_LIMIT_BYTES: usize = 4 * 1024 * 1024 * 1024 - 512 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    #[must_use]
    pub const fn new(values: Vec<Value>) -> Self {
        Self(values)
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RowVisitor;

        impl<'de> Visitor<'de> for RowVisitor {
            type Value = Row;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("A serialized JsonArray or JSON object is expected")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some((_, v)) = map.next_entry::<String, Value>()? {
                    values.push(v);
                }
                Ok(Row(values))
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(v) = seq.next_element::<Value>()? {
                    values.push(v);
                }
                Ok(Row(values))
            }
        }

        deserializer.deserialize_any(RowVisitor)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub data_format: String,
    pub schema: String,
    #[serde(skip)]
    pub id: QueryRecordId,
    #[serde(skip)]
    pub batch_size_bytes: usize,
    #[serde(skip)]
    pub configured_rows_limit: Option<usize>,
}

impl ResultSet {
    #[must_use]
    pub const fn calc_hard_rows_limit(&self) -> Option<usize> {
        if self.batch_size_bytes > QUERY_HISTORY_HARD_LIMIT_BYTES {
            let batch_size_bytes: i128 = self.batch_size_bytes as i128;
            let bytes_limit: i128 = QUERY_HISTORY_HARD_LIMIT_BYTES as i128;
            let limit_exceeded_bytes = batch_size_bytes - bytes_limit;
            let exceeded_in_percents = limit_exceeded_bytes / bytes_limit * 100;
            let shrink_on_count = if exceeded_in_percents > 50 {
                self.rows.len() * 90 / 100
            } else {
                self.rows.len() * 50 / 100
            };
            let hard_rows_limit: usize = if self.rows.len() > shrink_on_count {
                self.rows.len() - shrink_on_count
            } else {
                self.rows.len()
            };
            Some(hard_rows_limit)
        } else {
            None
        }
    }

    pub fn serialize_with_limit(&self) -> (std::result::Result<String, serde_json::Error>, usize) {
        let max_rows_limit = self.configured_rows_limit.unwrap_or(usize::MAX);
        let hard_rows_limit = self.calc_hard_rows_limit();
        let rows_limit = max_rows_limit.min(hard_rows_limit.unwrap_or(max_rows_limit));
        let serialize_rows_count = rows_limit.min(self.rows.len());
        (
            self.serialize_with_soft_limit(serialize_rows_count),
            serialize_rows_count,
        )
    }

    pub fn serialize_with_soft_limit(
        &self,
        n_rows: usize,
    ) -> std::result::Result<String, serde_json::Error> {
        let result_set_with_limit = LimitedResultSet {
            columns: &self.columns,
            rows: &self.rows[..self.rows.len().min(n_rows)],
            data_format: &self.data_format,
            schema: &self.schema,
        };
        serde_json::to_string(&result_set_with_limit)
    }
}

#[derive(Debug, Clone, Serialize)]
struct LimitedResultSet<'a> {
    pub columns: &'a [Column],
    pub rows: &'a [Row],
    pub data_format: &'a str,
    pub schema: &'a str,
}

impl TryFrom<Bytes> for ResultSet {
    type Error = ex_error::Error;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let result_str = String::from_utf8(value.to_vec()).context(ex_error::Utf8Snafu)?;
        let result_set: Self =
            serde_json::from_str(&result_str).context(ex_error::SerdeParseSnafu)?;
        Ok(result_set)
    }
}
