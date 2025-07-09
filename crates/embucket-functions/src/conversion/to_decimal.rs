use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;

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
    #[must_use]
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
            aliases: vec![
                "to_number".to_string(),
                "try_to_number".to_string(),
                "to_numeric".to_string(),
                "try_to_numeric".to_string(),
            ],
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

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        //TODO: Correct error type
        internal_err!("return_type_from_args should be called")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        match args.arg_types.len() {
            1 => {
                //Variant json null -> null, not only for try_to_decimal
                Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true))
            }
            2 => match &args.scalar_arguments[1] {
                Some(
                    ScalarValue::Utf8(..) | ScalarValue::Utf8View(..) | ScalarValue::LargeUtf8(..),
                ) => Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true)),
                Some(ScalarValue::Int64(Some(precession))) => {
                    let Ok(precession) = u8::try_from(*precession) else {
                        return internal_err!(
                            "invalid precession number (only allowed from 0 to 38): {}",
                            precession
                        );
                    };
                    Ok(ReturnInfo::new(DataType::Decimal128(precession, 0), true))
                }
                other => {
                    internal_err!("unexpected data type: {:?}", other)
                }
            },
            3 => match &args.scalar_arguments[1..=2] {
                [
                    Some(
                        ScalarValue::Utf8(..)
                        | ScalarValue::Utf8View(..)
                        | ScalarValue::LargeUtf8(..),
                    ),
                    Some(ScalarValue::Int64(Some(precession))),
                ] => {
                    let Ok(precession) = u8::try_from(*precession) else {
                        return internal_err!(
                            "invalid precession number (only allowed from 0 to 38): {}",
                            precession
                        );
                    };
                    Ok(ReturnInfo::new(DataType::Decimal128(precession, 0), true))
                }
                [
                    Some(ScalarValue::Int64(Some(precession))),
                    Some(ScalarValue::Int64(Some(scale))),
                ] => {
                    let Ok(precession) = u8::try_from(*precession) else {
                        return internal_err!(
                            "invalid precession number (only allowed from 0 to 38): {}",
                            precession
                        );
                    };
                    let Ok(scale) = i8::try_from(*scale) else {
                        return internal_err!(
                            "invalid precession number (only allowed from 0 to 1): {}",
                            scale
                        );
                    };
                    Ok(ReturnInfo::new(
                        DataType::Decimal128(precession, scale),
                        true,
                    ))
                }
                [other1, other2] => {
                    internal_err!("unexpected data type: {:?}, {:?}", other1, other2)
                }
                [..] => unreachable!(),
            },
            4 => match &args.scalar_arguments[2..=3] {
                [
                    Some(ScalarValue::Int64(Some(precession))),
                    Some(ScalarValue::Int64(Some(scale))),
                ] => {
                    let Ok(precession) = u8::try_from(*precession) else {
                        return internal_err!(
                            "invalid precession number (only allowed from 0 to 38): {}",
                            precession
                        );
                    };
                    let Ok(scale) = i8::try_from(*scale) else {
                        return internal_err!(
                            "invalid precession number (only allowed from 0 to 1): {}",
                            scale
                        );
                    };
                    Ok(ReturnInfo::new(
                        DataType::Decimal128(precession, scale),
                        true,
                    ))
                }
                [other1, other2] => {
                    internal_err!("unexpected data type: {:?}, {:?}", other1, other2)
                }
                [..] => unreachable!(),
            },
            other => {
                internal_err!("too many or too little args number: {other}")
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

crate::macros::make_udf_function!(ToDecimalFunc);
