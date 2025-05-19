use super::with_derives;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde_json::Value;
#[cfg(feature = "schema")]
use utoipa::ToSchema;

use super::column_info::ColumnInfo;
use arrow::array::RecordBatch;
use arrow::json::{WriterBuilder, writer::JsonArray};
use indexmap::IndexMap;
use snafu::ResultExt;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ResultSetError {
    #[snafu(display("Failed to parse row JSON: {source}"))]
    ResultParse { source: serde_json::Error },

    #[snafu(display("ResultSet create error: {source}"))]
    CreateResultSet { source: arrow::error::ArrowError },

    #[snafu(display("Error encoding UTF8 string: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct Column {
        pub name: String,
        pub r#type: String,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "schema", schema(as = Row, value_type = Vec<Value>))]
    pub struct Row(Vec<Value>);
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct ResultSet {
        pub columns: Vec<Column>,
        pub rows: Vec<Row>,
    }
}

impl ResultSet {
    pub fn query_result_to_result_set(
        records: &[RecordBatch],
        columns: &[ColumnInfo],
    ) -> std::result::Result<Self, ResultSetError> {
        let buf = Vec::new();
        let write_builder = WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = write_builder.build::<_, JsonArray>(buf);

        // serialize records to str
        let records: Vec<&RecordBatch> = records.iter().collect();
        writer
            .write_batches(&records)
            .context(CreateResultSetSnafu)?;
        writer.finish().context(CreateResultSetSnafu)?;

        // Get the underlying buffer back,
        let buf = writer.into_inner();
        let record_batch_str = String::from_utf8(buf).context(Utf8Snafu)?;

        // convert to array, leaving only values
        let rows: Vec<IndexMap<String, Value>> =
            serde_json::from_str(record_batch_str.as_str()).context(ResultParseSnafu)?;
        let rows: Vec<Row> = rows
            .into_iter()
            .map(|obj| Row(obj.values().cloned().collect()))
            .collect();

        let columns = columns
            .iter()
            .map(|ci| Column {
                name: ci.name.clone(),
                r#type: ci.r#type.clone(),
            })
            .collect();

        Ok(Self { columns, rows })
    }
}

impl TryFrom<&str> for ResultSet {
    type Error = ResultSetError;

    fn try_from(result: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(result).context(ResultParseSnafu)
    }
}
