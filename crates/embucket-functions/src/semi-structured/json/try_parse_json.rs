use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;

#[derive(Debug)]
pub struct TryParseJsonFunc {
    signature: Signature,
}

impl Default for TryParseJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryParseJsonFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(1), TypeSignature::String(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TryParseJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "try_parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let result = args.args[0].clone();
        Ok(result)
    }
}

crate::macros::make_udf_function!(TryParseJsonFunc);
