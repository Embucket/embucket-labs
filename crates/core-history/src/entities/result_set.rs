use crate::QueryRecordId;
use crate::errors;
use bytes::Bytes;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::fmt;

// ResultSet should not exceeded limit of 4GB - 512MB
pub const QUERY_HISTORY_HARD_LIMIT_BYTES: usize = 4 * 1024 * 1024 * 1024 - 512 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

/// Row uses custom desrializer to deserialize
/// `JsonArray` `[{"col":1,"col":2}, {"col":1,"col":2}]` omiting keys
/// or `JsonArray` `[1, 2]`, to the `Vec<Value>`
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    #[must_use]
    pub const fn new(values: Vec<Value>) -> Self {
        Self(values)
    }
}

/// `<https://github.com/embucket/embucket-labs/issues/1662`>
/// Custom deserializer for deserializing `RecordBatch` rows having duplicate columns names
/// like this: `[{"col":1,"col":2}, {"col":1,"col":2}]`, into the `Vec<Value>` (omiting keys).
/// It also support deserializng `JsonArray` `[1, 2]`, to the `Vec<Value>`
/// Original desrializer was using `IndexMap<String, Value>` causing columns data loss.
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

        // allows Serde to dispatch to map or seq depending on input
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
    // we need this before result set saved to history, do not even serialize it otherwise
    #[serde(skip)]
    pub batch_size_bytes: usize,
    #[serde(skip)]
    pub configured_rows_limit: Option<usize>,
}

impl ResultSet {
    #[tracing::instrument(
        level = "info",
        name = "ResultSet::serialized_result_set",
        skip(self),
        fields(serialized_count, serialization_error)
    )]
    pub fn as_serialized(&self) -> Option<(Bytes, usize)> {
        let (encoding_res, serialized_rows_count) = self.serialize_with_limit();
        match encoding_res {
            Ok(serialized_result) => {
                tracing::Span::current().record("serialized_count", serialized_rows_count);
                return Some((Bytes::from(serialized_result), serialized_rows_count));
            }
            Err(err) => {
                tracing::Span::current().record("serialization_error", format!("{err:?}"));
                tracing::error!("Failed to serialize result set: {err:?}");
            }
        }
        None
    }

    #[must_use]
    #[allow(clippy::as_conversions)]
    pub const fn calc_hard_rows_limit(&self) -> Option<usize> {
        if self.batch_size_bytes > QUERY_HISTORY_HARD_LIMIT_BYTES {
            // assign limit to bigger data type to avoid overflow at substruction
            let batch_size_bytes: i128 = self.batch_size_bytes as i128;
            let bytes_limit: i128 = QUERY_HISTORY_HARD_LIMIT_BYTES as i128;
            let limit_exceeded_bytes = batch_size_bytes - bytes_limit;
            // how many bytes exceeded in percents (no fractions needed):
            let exceeded_in_percents = limit_exceeded_bytes / bytes_limit * 100;

            let shrink_on_count = if exceeded_in_percents > 50 {
                self.rows.len() * 90 / 100 // shrink 90 % of rows
            } else {
                self.rows.len() * 50 / 100 // shrink 50 % of rows
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
    /// This takes result by reference, since it just serialize it, so can't be consumed
    /// Checks if upper bytes limit exceeded and apply rows limit, also if no hard limit break
    /// limit rows count using specified argument.
    #[tracing::instrument(
        name = "ResultSet::serialize_with_limit",
        level = "debug",
        fields(batch_size_bytes=self.batch_size_bytes, rows_count=self.rows.len(), serialized_size, rows_count_limit),
    )]
    pub fn serialize_with_limit(&self) -> (std::result::Result<String, serde_json::Error>, usize) {
        let max_rows_limit = self.configured_rows_limit.unwrap_or(usize::MAX);

        // Check if hard limit is exceeded by size or rows count
        let hard_rows_limit = self.calc_hard_rows_limit();
        let rows_limit = max_rows_limit.min(hard_rows_limit.unwrap_or(max_rows_limit));
        let serialize_rows_count = rows_limit.min(self.rows.len());

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("hard_rows_limit", hard_rows_limit)
            .record("serialize_rows_count", serialize_rows_count);

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

// Use this struct internally for slices on rows, and serialization
#[derive(Debug, Clone, Serialize)]
struct LimitedResultSet<'a> {
    pub columns: &'a [Column],
    pub rows: &'a [Row],
    pub data_format: &'a str,
    pub schema: &'a str,
}

impl TryFrom<Bytes> for ResultSet {
    type Error = errors::Error;
    #[tracing::instrument(name = "ResultSet::try_from", level = "error", err)]
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let result_str = String::from_utf8(value.to_vec()).context(errors::BadRawResultSetSnafu)?;
        let result_set: Self =
            serde_json::from_str(&result_str).context(errors::DeserializeValueSnafu)?;
        Ok(result_set)
    }
}
