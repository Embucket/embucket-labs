use datafusion::arrow::array::{StringBuilder, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::{DataFusionError, ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};

#[derive(Debug)]
pub struct GetPathFunc {
    signature: Signature,
}

impl Default for GetPathFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GetPathFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GetPathFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "get_path"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        match (&args[0], &args[0]) {
            (ColumnarValue::Array(arr), ColumnarValue::Scalar(ScalarValue::Utf8(Some(path)))) => {
                let input = as_string_array(arr);
                let mut res = StringBuilder::new();
                for v in input {
                    if let Some(v) = v {
                        match serde_json::from_str::<serde_json::Value>(v) {
                            Ok(json_value) => {
                                let value = jsonpath_lib::select(&json_value, path)
                                    .map_err(|e| exec_err!("can't parse jsonpath: {}", e))?;
                                let value = json_value.pointer(path);
                                if let Some(value) = value {
                                    res.append_value(value.to_string());
                                } else {
                                    res.append_null();
                                }
                            }
                            Err(_) => res.append_null(),
                        }
                    } else {
                        res.append_null();
                    }
                }
            }
        }
        let input = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let path = match args[1].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let input = as_string_array(&input);
    }
}
