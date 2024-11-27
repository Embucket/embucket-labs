// use std::any::Any;
// use std::sync::Arc;
//
// use arrow::array::{ArrayRef, ListArray, StringArray, StringBuilder};
// use arrow::datatypes::{DataType, Field};
// use datafusion::common::{exec_err, Result};
// use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
// use serde_json::Value;
//
// #[derive(Debug)]
// pub struct JsonExtractArray {
//     signature: Signature,
// }
//
// impl Default for JsonExtractArray {
//     fn default() -> Self {
//         Self::new()
//     }
// }
//
// impl JsonExtractArray {
//     pub fn new() -> Self {
//         Self {
//             signature: Signature::string(1, Volatility::Immutable),
//         }
//     }
// }
//
// impl ScalarUDFImpl for JsonExtractArray {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//
//     fn name(&self) -> &str {
//         "json_extract_array"
//     }
//
//     fn signature(&self) -> &Signature {
//         &self.signature
//     }
//
//     fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
//         Ok(DataType::List(Box::new(Field::new("item", DataType::Utf8, true))))
//     }
//
//     fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
//         if args.len() != 1 {
//             return exec_err!("json_extract_array function requires one argument, got {}", args.len());
//         }
//
//         let json_array = match &args[0] {
//             ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringArray>().unwrap(),
//             _ => return exec_err!("json_extract_array function requires a StringArray argument"),
//         };
//
//         let mut list_builder = ListArray::builder(StringBuilder::new(json_array.len()));
//
//         for i in 0..json_array.len() {
//             if json_array.is_null(i) {
//                 list_builder.append(false)?;
//             } else {
//                 let json_str = json_array.value(i);
//                 let parsed: Value = serde_json::from_str(json_str).unwrap();
//                 if let Value::Array(arr) = parsed {
//                     for item in arr {
//                         if let Value::String(s) = item {
//                             list_builder.values().append_value(&s)?;
//                         } else {
//                             list_builder.values().append_null()?;
//                         }
//                     }
//                 }
//                 list_builder.append(true)?;
//             }
//         }
//
//         let list_array = Arc::new(list_builder.finish()) as ArrayRef;
//         Ok(ColumnarValue::Array(list_array))
//     }
// }
