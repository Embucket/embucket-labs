use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility};
use datafusion_expr::{ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use datafusion_common::internal_err;
use datafusion_common::types::{logical_float16, logical_float32, logical_float64, logical_int8, logical_string, logical_uint32, logical_uint8, NativeType};

#[derive(Debug)]
pub struct ToDecimalFunc {
    signature: Signature,
    aliases: Vec<String>,
    r#try: bool,
}

impl Default for ToDecimalFunc {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ToDecimalFunc {
    pub fn new(r#try: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    //TO_DECIMAL( <expr> )
                    TypeSignature::Any(1),
                    //TO_DECIMAL( <expr> [, '<format>' ] )
                    //TO_DECIMAL( <expr> [, <precision> ] )
                    TypeSignature::Any(2),
                    //TO_DECIMAL( <expr> [, '<format>' ] [, <precision> ] )
                    //TO_DECIMAL( <expr> [, <precision> [, <scale> ] ] )
                    TypeSignature::Any(3),
                    //TO_DECIMAL( <expr> [, '<format>' ] [, <precision> [, <scale> ] ] )
                    TypeSignature::Any(4),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["to_number".to_string(), "try_to_number".to_string(), "to_numeric".to_string(), "try_to_numeric".to_string()],
            r#try,
        }
    }
}

impl ScalarUDFImpl for ToDecimalFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.r#try {
            "try_to_decimal"
        } else {
            "to_decimal"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        //TODO: Correct error type
        internal_err!("return_type_from_args should be called")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        todo!()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        todo!()
    }
}

crate::macros::make_udf_function!(ToDecimalFunc);