use datafusion::arrow::array::as_string_array;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
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

        match (&args[0],&args[0]) {
            
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
