use crate::entities::result_set::{Column, Row, ResultSet, RESULT_SET_LIMIT_THRESHOULD_BYTES};
use serde_json::{Value, Number};
use tokio;

#[tokio::test]
async fn test_query_record_exceeds_limit() {   
    // not enough rows to shrink
    assert_eq!(
        ResultSet {
            columns: vec![Column { name: "col1".to_string(), r#type: "int".to_string() }],
            rows: vec![Row(vec![Value::Number(Number::from(1))])],
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: RESULT_SET_LIMIT_THRESHOULD_BYTES + 1,
        }.serialize_with_auto_limit().1,
        1
    );

    // shrink 50 % of rows
    assert_eq!(
        ResultSet {
            columns: vec![Column { name: "col1".to_string(), r#type: "int".to_string() }],
            rows: (0..10).map(|i| Row(vec![Value::Number(Number::from(i))])).collect(),
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: RESULT_SET_LIMIT_THRESHOULD_BYTES + 1,
        }.serialize_with_auto_limit().1,
        5
    );

    // shrink 90 % of rows
    assert_eq!(
        ResultSet {
            columns: vec![Column { name: "col1".to_string(), r#type: "int".to_string() }],
            rows: (0..10).map(|i| Row(vec![Value::Number(Number::from(i))])).collect(),
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: RESULT_SET_LIMIT_THRESHOULD_BYTES * 2,
        }.serialize_with_auto_limit().1,
        1
    );      
}