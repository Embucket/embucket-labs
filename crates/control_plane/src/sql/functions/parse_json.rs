use std::any::Any;

use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
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
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
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

    fn name(&self) -> &str {
        "parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Define the return type as a Struct with dynamic fields
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("key1", DataType::Utf8, true),
            Field::new("key2", DataType::Utf8, true),
        ])))
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        todo!()
    }

    // fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
    //     if args.len() != 1 {
    //         return exec_err!("parse_json function requires one argument, got {}", args.len());
    //     }
    //
    //     let json_array = match &args[0] {
    //         ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringArray>().unwrap(),
    //         _ => return exec_err!("parse_json function requires a StringArray argument"),
    //     };
    //
    //     let mut key1_builder = StringArray::new(json_array.len());
    //     let mut key2_builder = StringArray::builder(json_array.len());
    //
    //     for i in 0..json_array.len() {
    //         if json_array.is_null(i) {
    //             key1_builder.append_null()?;
    //             key2_builder.append_null()?;
    //         } else {
    //             let json_str = json_array.value(i);
    //             let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    //             key1_builder.append_value(parsed["key1"].as_str().unwrap_or(""))?;
    //             key2_builder.append_value(parsed["key2"].as_str().unwrap_or(""))?;
    //         }
    //     }
    //
    //     let key1_array = Arc::new(key1_builder.finish()) as ArrayRef;
    //     let key2_array = Arc::new(key2_builder.finish()) as ArrayRef;
    //
    //     let struct_array = StructArray::from(vec![
    //         ("key1", key1_array),
    //         ("key2", key2_array),
    //     ]);
    //
    //     Ok(ColumnarValue::Array(Arc::new(struct_array)))
    // }
}