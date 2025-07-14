use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::arrow::array::{Array, StringArray};
use datafusion_common::arrow::compute::CastOptions;
use datafusion_common::arrow::util::display::FormatOptions;
use datafusion_common::{ScalarValue, exec_err, internal_err};
use datafusion_expr::{ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::fmt::Debug;
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
                Some(ScalarValue::Int64(Some(precision))) => {
                    let Ok(precision) = u8::try_from(*precision) else {
                        return internal_err!(
                            "invalid precision number (only allowed from 0 to 38): {}",
                            precision
                        );
                    };
                    Ok(ReturnInfo::new(DataType::Decimal128(precision, 0), true))
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
                    Some(ScalarValue::Int64(Some(precision))),
                ] => {
                    let Ok(precision) = u8::try_from(*precision) else {
                        return internal_err!(
                            "invalid precision number (only allowed from 0 to 38): {}",
                            precision
                        );
                    };
                    Ok(ReturnInfo::new(DataType::Decimal128(precision, 0), true))
                }
                [
                    Some(ScalarValue::Int64(Some(precision))),
                    Some(ScalarValue::Int64(Some(scale))),
                ] => {
                    let Ok(precision) = u8::try_from(*precision) else {
                        return internal_err!(
                            "invalid precision number (only allowed from 0 to 38): {}",
                            precision
                        );
                    };
                    let Ok(scale) = i8::try_from(*scale) else {
                        return internal_err!(
                            "invalid precision number (only allowed from 0 to <precision> - 1): {}",
                            scale
                        );
                    };
                    Ok(ReturnInfo::new(
                        DataType::Decimal128(precision, scale),
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
                    Some(ScalarValue::Int64(Some(precision))),
                    Some(ScalarValue::Int64(Some(scale))),
                ] => {
                    let Ok(precision) = u8::try_from(*precision) else {
                        return internal_err!(
                            "invalid precision number (only allowed from 0 to 38): {}",
                            precision
                        );
                    };
                    let Ok(scale) = i8::try_from(*scale) else {
                        return internal_err!(
                            "invalid precision number (only allowed from 0 to <precision> - 1): {}",
                            scale
                        );
                    };
                    Ok(ReturnInfo::new(
                        DataType::Decimal128(precision, scale),
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
        let DataType::Decimal128(precision, scale) = args.return_type else {
            return exec_err!("unexpected return type");
        };

        let expr = &args.args[0];
        let array = match expr {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        let format = &args.args[1];
        let format = match format {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(str))
                | ScalarValue::Utf8View(Some(str))
                | ScalarValue::LargeUtf8(Some(str)),
            ) => Some(str.as_str()),
            ColumnarValue::Scalar(
                ScalarValue::Int64(Some(_))
                | ScalarValue::Int32(Some(_))
                | ScalarValue::Int16(Some(_))
                | ScalarValue::Int8(Some(_))
                | ScalarValue::UInt64(Some(_))
                | ScalarValue::UInt32(Some(_))
                | ScalarValue::UInt16(Some(_))
                | ScalarValue::UInt8(Some(_))
                | ScalarValue::Float64(Some(_))
                | ScalarValue::Float32(Some(_)),
            ) => None,
            ColumnarValue::Array(_) | ColumnarValue::Scalar(_) => {
                return exec_err!("unexpected second argument format");
            }
        };

        //TODO: should we have type info as before, good datapoint to think about on other types, functions, etc
        let format_options = FormatOptions::default()
            //TODO: override NULL formatting is not working, expected? Visitor somewhere?
            .with_null("NULL")
            .with_types_info(false);

        let result_array = match array.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                let array = match format {
                    Some(format) => {
                        //TODO: needs logic for binary string with binary formatting and variant types
                        let array: &StringArray = array.as_any().downcast_ref().unwrap();
                        let values: Vec<_> = array.iter().collect();
                        let values = if format.starts_with('$') {
                            values
                                .iter()
                                .map(|opt| {
                                    opt.map(|str| {
                                        str.strip_prefix('$')
                                            .map_or_else(|| str, |stripped| stripped)
                                    })
                                })
                                .collect()
                        } else {
                            values
                        };
                        let values: Vec<_> = if format.contains(',') {
                            values
                                .iter()
                                .map(|opt| opt.as_ref().map(|str| str.replace(',', "")))
                                .collect()
                        } else {
                            values
                                .iter()
                                .map(|opt| opt.map(std::string::ToString::to_string))
                                .collect()
                        };

                        Arc::new(StringArray::from(values))
                    }
                    None => array.clone(),
                };

                cast_with_options(
                    &array,
                    &DataType::Decimal128(*precision, *scale),
                    &CastOptions {
                        safe: self.r#try,
                        format_options,
                    },
                )?
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
            | DataType::Float64 => cast_with_options(
                array,
                &DataType::Decimal128(*precision, *scale),
                &CastOptions {
                    safe: self.r#try,
                    format_options,
                },
            )?,
            other => return exec_err!("unexpected data type: {:?}", other),
        };
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
