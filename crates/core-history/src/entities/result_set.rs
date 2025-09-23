use crate::QueryRecord;
use crate::errors;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde::de::{Deserializer, MapAccess, Visitor};
use std::fmt;
use snafu::{OptionExt, ResultExt};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    #[must_use]
    pub const fn new(values: Vec<Value>) -> Self {
        Self(values)
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
    fn try_from(value: QueryRecord) -> Result<Self, Self::Error> {
        let result_str = value
            .result
            .context(errors::NoResultSetSnafu { query_id: value.id })?;

        let result_set: Self =
            serde_json::from_str(&result_str).context(errors::DeserializeValueSnafu)?;
        Ok(result_set)
    }
}


#[derive(Debug)]
pub struct RowData(pub Row);

/// https://github.com/Embucket/embucket/issues/1662
/// Custom deserializer for deserializing RecordBatch rows having duplicate columns names
/// like this: `[{"col":1,"col":2}, {"col":1,"col":2}]`, into the Vec<Value>.
/// Original desrializer was using `IndexMap<String, Value>` causing columns data loss.
impl<'de> Deserialize<'de> for RowData {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RowDataVisitor;

        impl<'de> Visitor<'de> for RowDataVisitor {
            type Value = RowData;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("A serialized JsonArray is expected")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some((_, v)) = map.next_entry::<String, Value>()? {
                    values.push(v);
                }
                Ok(RowData(Row(values)))
            }
        }

        deserializer.deserialize_map(RowDataVisitor)
    }
}