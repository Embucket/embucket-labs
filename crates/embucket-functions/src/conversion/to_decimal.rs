use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;
use datafusion_common::types::{logical_float64, logical_int8, logical_string, logical_uint32, logical_uint8, NativeType};

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
                    TypeSignature::Coercible(vec![
                        Self::expr_arg_type(),
                    ]),
                    //TO_DECIMAL( <expr> [, '<format>' ] )
                    TypeSignature::Coercible(vec![
                        Self::expr_arg_type(),
                        Self::format_arg_type(),
                    ]),
                    //TO_DECIMAL( <expr> [, <precision> ] )
                    TypeSignature::Coercible(vec![
                        Self::expr_arg_type(),
                        Self::precision_arg_type(),
                    ]),
                    //TO_DECIMAL( <expr> [, <precision> [, <scale> ] ] )
                    TypeSignature::Coercible(vec![
                        Self::expr_arg_type(),
                        Self::precision_arg_type(),
                        Self::scale_arg_type(),
                    ]),
                    //TO_DECIMAL( <expr> [, '<format>' ] [, <precision> ] )
                    TypeSignature::Coercible(vec![
                        Self::expr_arg_type(),
                        Self::format_arg_type(),
                        Self::precision_arg_type(),
                    ]),
                    //TO_DECIMAL( <expr> [, '<format>' ] [, <precision> [, <scale> ] ] )
                    TypeSignature::Coercible(vec![
                        Self::expr_arg_type(),
                        Self::format_arg_type(),
                        Self::precision_arg_type(),
                        Self::scale_arg_type(),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["to_number".to_string(), "try_to_number".to_string(), "to_numeric".to_string(), "try_to_numeric".to_string()],
            r#try,
        }
    }
    fn expr_arg_type() -> Coercion {
        //Needs numeric -> decimal type
        Coercion::new_implicit(
            TypeSignatureClass::Native(logical_string()),
            vec![TypeSignatureClass::Integer, TypeSignatureClass::Native(logical_float64()), TypeSignatureClass::Native(logical_uint32())],
            NativeType::String,
        )
    }

    fn format_arg_type() -> Coercion {
        Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
    }

    fn precision_arg_type() -> Coercion {
        Coercion::new_exact(TypeSignatureClass::Native(logical_uint8()))
    }

    fn scale_arg_type() -> Coercion {
        Coercion::new_exact(TypeSignatureClass::Native(logical_int8()))
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

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        todo!()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        todo!()
    }
}

crate::macros::make_udf_function!(ToDecimalFunc);