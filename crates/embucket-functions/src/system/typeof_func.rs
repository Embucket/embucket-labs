use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SystemTypeofFunc {
    signature: Signature,
}

impl Default for SystemTypeofFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemTypeofFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SystemTypeofFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "system$typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = &args[0];
        let dtype = arg.data_type();
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(format!(
            "{dtype}"
        )))))
    }
}

crate::macros::make_udf_function!(SystemTypeofFunc);
