use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ParseJsonFunc {
    signature: Signature,
}

impl Default for ParseJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseJsonFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

// static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
//
// fn get_doc() -> &'static Documentation {
//     DOCUMENTATION.get_or_init(|| {
//         Documentation::builder()
//             .with_description("Parses a JSON string and extracts values for key1 and key2")
//             .with_syntax_example("parse_json('{\"key1\": \"value1\", \"key2\": \"value2\"}')")
//             .with_argument("arg1", "The JSON string to parse")
//             .build()
//             .unwrap()
//     })
// }

impl ScalarUDFImpl for ParseJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    #[tracing::instrument(level = "trace", skip(self, args))]
    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!(
                "parse_json function requires one argument, got {}",
                args.len()
            );
        }
        let result = args[0].clone();
        Ok(result)
        // parse_raw_json(&args[0])
    }
}

#[allow(dead_code)]
fn parse_raw_json(json_str: &ColumnarValue) -> Result<ColumnarValue> {
    let result = match json_str {
        ColumnarValue::Array(array) => {
            #[allow(clippy::unwrap_used)]
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            if string_array.len() != 1 {
                return exec_err!("Expected a single JSON string");
            }
            string_array.value(0)
        }
        ColumnarValue::Scalar(scalar) => {
            if let ScalarValue::Utf8(Some(value)) = scalar {
                value
            } else {
                return exec_err!("Expected a UTF-8 string scalar");
            }
        }
    };
    match serde_json::from_str::<Value>(result) {
        Ok(parsed) => {
            let result = json_value_to_columnar_value(&parsed);
            Ok(result)
        }
        Err(e) => exec_err!("Failed to parse JSON string: {}", e),
    }
}

fn json_value_to_array_ref(value: &Value) -> ArrayRef {
    match value {
        Value::Null => Arc::new(StringArray::from(vec![None::<&str>])),
        Value::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
        Value::Number(num) =>
        {
            #[allow(clippy::option_if_let_else)]
            if let Some(i) = num.as_i64() {
                Arc::new(Int64Array::from(vec![i]))
            } else if let Some(f) = num.as_f64() {
                Arc::new(Float64Array::from(vec![f]))
            } else {
                Arc::new(Float64Array::from(vec![None::<f64>]))
            }
        }
        Value::String(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Value::Array(arr) => {
            let arrays: Vec<ArrayRef> = arr.iter().map(json_value_to_array_ref).collect();
            let fields: Vec<Field> = arrays
                .iter()
                .enumerate()
                .map(|(i, v)| Field::new(format!("field_{i}"), v.data_type().clone(), true))
                .collect();
            let struct_array = StructArray::new(Fields::from(fields), arrays, None);
            Arc::new(struct_array)
        }
        Value::Object(obj) => {
            let fields: Vec<Field> = obj
                .iter()
                .map(|(k, v)| Field::new(k, json_value_to_data_type(v), true))
                .collect();
            let arrays: Vec<ArrayRef> = obj.values().map(json_value_to_array_ref).collect();
            let struct_array = StructArray::new(Fields::from(fields), arrays, None);
            Arc::new(struct_array)
        }
    }
}

fn json_value_to_data_type(value: &Value) -> DataType {
    match value {
        Value::Null | Value::String(_) => DataType::Utf8,
        Value::Bool(_) => DataType::Boolean,
        Value::Number(num) => {
            if num.as_i64().is_some() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        Value::Array(arr) => {
            let fields: Vec<Field> = arr
                .iter()
                .enumerate()
                .map(|(i, v)| Field::new(format!("field_{i}"), json_value_to_data_type(v), true))
                .collect();
            DataType::Struct(Fields::from(fields))
        }
        Value::Object(obj) => {
            let fields: Vec<Field> = obj
                .iter()
                .map(|(k, v)| Field::new(k, json_value_to_data_type(v), true))
                .collect();
            DataType::Struct(Fields::from(fields))
        }
    }
}

fn json_value_to_columnar_value(value: &Value) -> ColumnarValue {
    match value {
        Value::Null => ColumnarValue::Scalar(ScalarValue::Null),
        Value::Bool(b) => ColumnarValue::Scalar(ScalarValue::Boolean(Some(*b))),
        Value::Number(num) =>
        {
            #[allow(clippy::option_if_let_else)]
            if let Some(i) = num.as_i64() {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i)))
            } else if let Some(f) = num.as_f64() {
                ColumnarValue::Scalar(ScalarValue::Float64(Some(f)))
            } else {
                ColumnarValue::Scalar(ScalarValue::Float64(None))
            }
        }
        Value::String(s) => ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.clone()))),
        Value::Array(arr) => {
            let arrays: Vec<ArrayRef> = arr.iter().map(json_value_to_array_ref).collect();
            let fields: Vec<Field> = arrays
                .iter()
                .enumerate()
                .map(|(i, v)| Field::new(format!("field_{i}"), v.data_type().clone(), true))
                .collect();
            let struct_array = StructArray::new(Fields::from(fields), arrays, None);
            ColumnarValue::Array(Arc::new(struct_array))
        }
        Value::Object(obj) => {
            let fields: Vec<Field> = obj
                .iter()
                .map(|(k, v)| Field::new(k, json_value_to_data_type(v), true))
                .collect();
            let arrays: Vec<ArrayRef> = obj.values().map(json_value_to_array_ref).collect();
            let struct_array = StructArray::new(Fields::from(fields), arrays, None);
            ColumnarValue::Array(Arc::new(struct_array))
        }
    }
}

// TODO: Revisit this function
#[allow(dead_code)]
fn parse_data_type(data_type_str: &str) -> DataType {
    if data_type_str.starts_with("array<struct<") && data_type_str.ends_with(">>") {
        let struct_fields_str = &data_type_str["array<struct<".len()..data_type_str.len() - 2];
        let fields: Vec<Field> = struct_fields_str
            .split(',')
            .map(|field_str| {
                let parts: Vec<&str> = field_str.split(':').collect();
                Field::new(parts[0], DataType::Utf8, true)
            })
            .collect();
        let struct_field = Field::new("struct", DataType::Struct(Fields::from(fields)), true);
        DataType::List(Arc::new(struct_field))
    } else if data_type_str.starts_with("struct<") && data_type_str.ends_with('>') {
        let struct_fields_str = &data_type_str["struct<".len()..data_type_str.len() - 1];
        let fields: Vec<Field> = struct_fields_str
            .split(',')
            .map(|field_str| {
                let parts: Vec<&str> = field_str.split(':').collect();
                Field::new(parts[0], DataType::Utf8, true)
            })
            .collect();
        DataType::Struct(Fields::from(fields))
    } else {
        DataType::Struct(Fields::empty())
    }
}

crate::macros::make_udf_function!(ParseJsonFunc);
