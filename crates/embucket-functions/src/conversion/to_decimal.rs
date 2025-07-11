use datafusion::arrow::array::Decimal128Builder;
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::arrow::array::{Array, Decimal128Array};
use datafusion_common::arrow::compute::CastOptions;
use datafusion_common::arrow::util::display::FormatOptions;
use datafusion_common::{ScalarValue, exec_err, internal_err};
use datafusion_expr::{ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ToDecimalFunc {
    signature: Signature,
    aliases: Vec<String>,
    r#try: bool,
}

impl ToDecimalFunc {
    #[must_use]
    pub fn new(r#try: bool, aliases: Vec<String>) -> Self {
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
            aliases,
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
    //TODO: fix errors + logic
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
    //TODO: formatting <format> second arg
    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let DataType::Decimal128(precission, scale) = args.return_type else {
            return exec_err!("unexpected return type");
        };

        let expr = &args.args[0];

        //let format = yadah yadah blah blah blah

        let array = match expr {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        let mut result = Decimal128Builder::with_capacity(array.len())
            .with_precision_and_scale(*precission, *scale)?;

        match array.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                //TODO: needs logic for binary and varint types
                let array = cast_with_options(
                    array,
                    &DataType::Decimal128(*precission, *scale),
                    &CastOptions {
                        safe: self.r#try,
                        format_options: FormatOptions::default(),
                    },
                )?;
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                result.append_array(array);
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => {
                let array = cast_with_options(
                    array,
                    &DataType::Decimal128(*precission, *scale),
                    &CastOptions {
                        safe: self.r#try,
                        format_options: FormatOptions::default(),
                    },
                )?;
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                result.append_array(array);
            }
            other => return exec_err!("unexpected data type: {:?}", other),
        }

        let result = result.finish();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
