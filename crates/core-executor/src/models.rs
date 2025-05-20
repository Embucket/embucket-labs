use api_structs::column_info::ColumnInfo as ColumnInfo_;
use datafusion::arrow::array::RecordBatch;

// to keep existing imports unchanged
pub type ColumnInfo = ColumnInfo_;

#[derive(Debug)]
pub struct QueryResultData {
    pub records: Vec<RecordBatch>,
    pub columns_info: Vec<ColumnInfo>,
    // query_id is QueryRecordId, but we won't add dependency on history crate here
    pub query_id: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
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
