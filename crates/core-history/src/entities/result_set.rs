use crate::QueryRecord;
use crate::errors;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
