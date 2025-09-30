use crate::entities::result_set::{Column, QUERY_HISTORY_HARD_LIMIT_BYTES, ResultSet, Row};
use serde_json::{Number, Value};
use tokio;

#[tokio::test]
async fn test_query_record_exceeds_limit() {
    // test hard limit: not enough rows to shrink
    assert_eq!(
        ResultSet {
            columns: vec![Column {
                name: "col1".to_string(),
                r#type: "int".to_string()
            }],
            rows: vec![Row(vec![Value::Number(Number::from(1))])],
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: QUERY_HISTORY_HARD_LIMIT_BYTES + 1,
        }
        .serialize_with_limit(50)
        .1,
        1
    );

    // test hard limit: shrink 50 % of rows
    assert_eq!(
        ResultSet {
            columns: vec![Column {
                name: "col1".to_string(),
                r#type: "int".to_string()
            }],
            rows: (0..10)
                .map(|i| Row(vec![Value::Number(Number::from(i))]))
                .collect(),
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: QUERY_HISTORY_HARD_LIMIT_BYTES + 1,
        }
        .serialize_with_limit(50)
        .1,
        5
    );

    // test hard limit: shrink 90 % of rows
    assert_eq!(
        ResultSet {
            columns: vec![Column {
                name: "col1".to_string(),
                r#type: "int".to_string()
            }],
            rows: (0..10)
                .map(|i| Row(vec![Value::Number(Number::from(i))]))
                .collect(),
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: QUERY_HISTORY_HARD_LIMIT_BYTES * 2,
        }
        .serialize_with_limit(50)
        .1,
        1 // shrinking to 1 row (90% shrink)
    );

    // test rows limit
    assert_eq!(
        ResultSet {
            columns: vec![Column {
                name: "col1".to_string(),
                r#type: "int".to_string()
            }],
            rows: (0..10)
                .map(|i| Row(vec![Value::Number(Number::from(i))]))
                .collect(),
            data_format: "json".to_string(),
            schema: "schema".to_string(),
            batch_size_bytes: 0,
        }
        .serialize_with_limit(5)
        .1,
        5
    );
}
