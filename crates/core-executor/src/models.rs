use crate::Result;
use crate::error as ex_error;
use crate::utils::{DataSerializationFormat, convert_record_batches, convert_struct_to_timestamp};
use arrow_schema::SchemaRef;
use core_history::result_set::{Column, ResultSet, Row};
use core_history::{QueryRecordId, QueryStatus};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::arrow::json::StructMode;
use datafusion::arrow::json::WriterBuilder;
use datafusion::arrow::json::reader::ReaderBuilder;
use datafusion::arrow::json::writer::JsonArray;
use datafusion_common::arrow::datatypes::Schema;
use embucket_functions::to_snowflake_datatype;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

pub struct AsyncQueryHandle {
    pub query_id: QueryRecordId,
    pub rx: oneshot::Receiver<QueryResultStatus>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct QueryContext {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub worksheet_id: Option<i64>,
    pub query_id: QueryRecordId,
    pub request_id: Option<Uuid>,
    pub ip_address: Option<String>,
    // async_query flag is not used
    // TODO: remove or use it
    pub async_query: bool,
}

impl QueryContext {
    #[must_use]
    pub fn new(
        database: Option<String>,
        schema: Option<String>,
        worksheet_id: Option<i64>,
    ) -> Self {
        Self {
            database,
            schema,
            worksheet_id,
            query_id: QueryRecordId::default(),
            request_id: None,
            ip_address: None,
            async_query: false,
        }
    }

    #[must_use]
    pub const fn with_query_id(mut self, new_id: QueryRecordId) -> Self {
        self.query_id = new_id;
        self
    }

    #[must_use]
    pub const fn with_request_id(mut self, new_id: Uuid) -> Self {
        self.request_id = Some(new_id);
        self
    }

    #[must_use]
    pub fn with_ip_address(mut self, ip_address: String) -> Self {
        self.ip_address = Some(ip_address);
        self
    }

    #[must_use]
    pub const fn with_async_query(mut self, async_query: bool) -> Self {
        self.async_query = async_query;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub records: Vec<RecordBatch>,
    /// The schema associated with the result.
    /// This is required to construct a valid response even when `records` are empty
    pub schema: Arc<ArrowSchema>,
    pub query_id: QueryRecordId,
}

impl QueryResult {
    pub fn as_row_set(&self, data_format: DataSerializationFormat) -> Result<Vec<Row>> {
        // Do conversions every time, as currently all the history records had conversions
        // for arrow format though were saved as json

        // Convert the QueryResult to RecordBatches using the specified serialization format
        // Add columns dbt metadata to each field
        // Since we have to store already converted data to history
        let record_batches = convert_record_batches(self, data_format)?;
        // Convert struct timestamp columns to string representation
        let record_batches = &convert_struct_to_timestamp(&record_batches)?;

        let record_batches = record_batches.iter().collect::<Vec<_>>();

        // Serialize the RecordBatches into a JSON string using Arrow's Writer
        let buffer = Vec::new();
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(buffer);

        writer
            .write_batches(&record_batches)
            .context(ex_error::ArrowSnafu)?;
        writer.finish().context(ex_error::ArrowSnafu)?;

        let json_bytes = writer.into_inner();
        let json_str = String::from_utf8(json_bytes).context(ex_error::Utf8Snafu)?;

        // Deserialize the JSON string into rows of values
        let rows =
            serde_json::from_str::<Vec<Row>>(&json_str).context(ex_error::SerdeParseSnafu)?;

        Ok(rows)
    }

    pub fn as_result_set(&self, query_history_rows_limit: Option<usize>) -> Result<ResultSet> {
        // Extract column metadata from the original QueryResult
        let columns = self
            .column_info()
            .iter()
            .map(|ci| Column {
                name: ci.name.clone(),
                r#type: ci.r#type.clone(),
            })
            .collect();

        // Serialize original Schema into a JSON string
        let schema = serde_json::to_string(&self.schema).context(ex_error::SerdeParseSnafu)?;
        let data_format = DataSerializationFormat::Json;
        Ok(ResultSet {
            // just for refrence
            id: self.query_id,
            columns,
            rows: self.as_row_set(data_format)?,
            batch_size_bytes: self
                .records
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum(),
            // move here value of data_format we  hardcoded earlier
            data_format: data_format.to_string(),
            schema,
            configured_rows_limit: query_history_rows_limit,
        })
    }
}

fn convert_resultset_to_arrow_json_lines(
    result_set: &ResultSet,
) -> std::result::Result<String, serde_json::Error> {
    let mut lines = String::new();
    for row in &result_set.rows {
        let json_value = serde_json::Value::Array(row.0.clone());
        lines.push_str(&serde_json::to_string(&json_value)?);
        lines.push('\n');
    }
    Ok(lines)
}

/// Convert historical query record to `QueryResult`
impl TryFrom<ResultSet> for QueryResult {
    type Error = crate::Error;
    fn try_from(result_set: ResultSet) -> std::result::Result<Self, Self::Error> {
        let arrow_json = convert_resultset_to_arrow_json_lines(&result_set)
            .context(ex_error::SerdeParseSnafu)?;

        // Parse schema from serialized JSON
        let schema_value =
            serde_json::from_str(&result_set.schema).context(ex_error::SerdeParseSnafu)?;

        let schema_ref: SchemaRef = schema_value;
        let json_reader = ReaderBuilder::new(schema_ref.clone())
            .with_struct_mode(StructMode::ListOnly)
            .build(Cursor::new(&arrow_json))
            .context(ex_error::ArrowSnafu)?;

        let batches = json_reader
            .collect::<arrow::error::Result<Vec<RecordBatch>>>()
            .context(ex_error::ArrowSnafu)?;

        Ok(Self {
            records: batches,
            schema: schema_ref,
            query_id: result_set.id,
        })
    }
}

impl QueryResult {
    #[must_use]
    pub const fn new(
        records: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
        query_id: QueryRecordId,
    ) -> Self {
        Self {
            records,
            schema,
            query_id,
        }
    }
    #[must_use]
    pub const fn with_query_id(mut self, new_id: QueryRecordId) -> Self {
        self.query_id = new_id;
        self
    }

    #[must_use]
    pub fn column_info(&self) -> Vec<ColumnInfo> {
        ColumnInfo::from_schema(&self.schema)
    }
}

#[derive(Debug)]
pub struct QueryResultStatus {
    pub query_result: std::result::Result<QueryResult, crate::Error>,
    pub status: QueryStatus,
}

// TODO: We should not have serde dependency here
// Instead it should be in api-snowflake-rest
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub nullable: bool,
    pub r#type: String,
    pub byte_length: Option<i32>,
    pub length: Option<i32>,
    pub scale: Option<i32>,
    pub precision: Option<i32>,
    pub collation: Option<String>,
}

impl ColumnInfo {
    #[must_use]
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("logicalType".to_string(), self.r#type.to_uppercase());
        metadata.insert(
            "precision".to_string(),
            self.precision.unwrap_or(38).to_string(),
        );
        metadata.insert("scale".to_string(), self.scale.unwrap_or(0).to_string());
        metadata.insert(
            "charLength".to_string(),
            self.length.unwrap_or(0).to_string(),
        );
        metadata
    }

    #[must_use]
    pub fn from_schema(schema: &Arc<Schema>) -> Vec<Self> {
        let mut column_infos = Vec::new();
        for field in schema.fields() {
            column_infos.push(Self::from_field(field));
        }
        column_infos
    }

    #[must_use]
    pub fn from_field(field: &Field) -> Self {
        let mut column_info = Self {
            name: field.name().clone(),
            database: String::new(), // TODO
            schema: String::new(),   // TODO
            table: String::new(),    // TODO
            nullable: field.is_nullable(),
            r#type: field.data_type().to_string(),
            byte_length: None,
            length: None,
            scale: None,
            precision: None,
            collation: None,
        };

        column_info.r#type = to_snowflake_datatype(field.data_type());
        match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                column_info.precision = Some(38);
                column_info.scale = Some(0);
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                column_info.precision = Some(38);
                column_info.scale = Some(16);
            }
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                column_info.precision = Some(i32::from(*precision));
                column_info.scale = Some(i32::from(*scale));
            }
            // Varchar, Char, Utf8
            DataType::Utf8 => {
                column_info.byte_length = Some(16_777_216);
                column_info.length = Some(16_777_216);
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                column_info.precision = Some(0);
                column_info.scale = Some(9);
            }
            DataType::Timestamp(unit, _) => {
                column_info.precision = Some(0);
                let scale = match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                column_info.scale = Some(scale);
            }
            DataType::Binary | DataType::BinaryView => {
                column_info.byte_length = Some(8_388_608);
                column_info.length = Some(8_388_608);
            }
            _ => {}
        }
        column_info
    }
}

#[cfg(test)]
mod tests {
    use crate::models::ColumnInfo;
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_column_info_from_field() {
        let field = Field::new("test_field", DataType::Int8, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert!(!column_info.nullable);

        let field = Field::new("test_field", DataType::Decimal128(1, 2), true);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert_eq!(column_info.precision.unwrap(), 1);
        assert_eq!(column_info.scale.unwrap(), 2);
        assert!(column_info.nullable);

        let field = Field::new("test_field", DataType::Boolean, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "boolean");

        let field = Field::new("test_field", DataType::Time32(TimeUnit::Second), false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "time");
        assert_eq!(column_info.precision.unwrap(), 0);
        assert_eq!(column_info.scale.unwrap(), 9);

        let field = Field::new("test_field", DataType::Date32, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "date");

        let units = [
            (TimeUnit::Second, 0),
            (TimeUnit::Millisecond, 3),
            (TimeUnit::Microsecond, 6),
            (TimeUnit::Nanosecond, 9),
        ];
        for (unit, scale) in units {
            let field = Field::new("test_field", DataType::Timestamp(unit, None), false);
            let column_info = ColumnInfo::from_field(&field);
            assert_eq!(column_info.name, "test_field");
            assert_eq!(column_info.r#type, "timestamp_ntz");
            assert_eq!(column_info.precision.unwrap(), 0);
            assert_eq!(column_info.scale.unwrap(), scale);
        }

        let field = Field::new("test_field", DataType::Binary, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "binary");
        assert_eq!(column_info.byte_length.unwrap(), 8_388_608);
        assert_eq!(column_info.length.unwrap(), 8_388_608);

        // Any other type
        let field = Field::new("test_field", DataType::Utf8View, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "text");
        assert_eq!(column_info.byte_length, None);
        assert_eq!(column_info.length, None);

        let floats = [
            (DataType::Float16, 16, true),
            (DataType::Float32, 16, true),
            (DataType::Float64, 16, true),
            (DataType::Float64, 17, false),
        ];
        for (float_datatype, scale, outcome) in floats {
            let field = Field::new("test_field", float_datatype, false);
            let column_info = ColumnInfo::from_field(&field);
            assert_eq!(column_info.name, "test_field");
            assert_eq!(column_info.r#type, "real");
            assert_eq!(column_info.precision.unwrap(), 38);
            if outcome {
                assert_eq!(column_info.scale.unwrap(), scale);
            } else {
                assert_ne!(column_info.scale.unwrap(), scale);
            }
        }
    }

    #[tokio::test]
    async fn test_to_metadata() {
        let column_info = ColumnInfo {
            name: "test_field".to_string(),
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            nullable: false,
            r#type: "fixed".to_string(),
            byte_length: Some(8_388_608),
            length: Some(8_388_608),
            scale: Some(0),
            precision: Some(38),
            collation: None,
        };
        let metadata = column_info.to_metadata();
        assert_eq!(metadata.get("logicalType"), Some(&"FIXED".to_string()));
        assert_eq!(metadata.get("precision"), Some(&"38".to_string()));
        assert_eq!(metadata.get("scale"), Some(&"0".to_string()));
        assert_eq!(metadata.get("charLength"), Some(&"8388608".to_string()));
    }
}
