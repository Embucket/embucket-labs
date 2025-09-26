use crate::QueryRecord;
use crate::errors;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
use std::fmt;

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

/// `<https://github.com/Embucket/embucket/issues/1662`>
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub data_format: String,
    pub schema: String,
}

impl TryFrom<QueryRecord> for ResultSet {
    type Error = errors::Error;
    #[tracing::instrument(name = "ResultSet::try_from", level = "error", err)]
    fn try_from(value: QueryRecord) -> Result<Self, Self::Error> {
        let result_str = value
            .result
            .context(errors::NoResultSetSnafu { query_id: value.id })?;

        let result_set: Self =
            serde_json::from_str(&result_str).context(errors::DeserializeValueSnafu)?;
        Ok(result_set)
    }
}
