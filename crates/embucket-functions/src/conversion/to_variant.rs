use crate::macros::make_udf_function;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;

#[derive(Debug)]
pub struct ToVariantFunc {
    signature: Signature,
}

impl Default for ToVariantFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToVariantFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToVariantFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_variant"
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

make_udf_function!(ToVariantFunc);
