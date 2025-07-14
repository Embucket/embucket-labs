use super::errors as conv_errors;
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{Array, StringArray};
use datafusion_common::arrow::compute::CastOptions;
use datafusion_common::arrow::util::display::FormatOptions;
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
    fn get_precision_checked(precision_scalar: &ScalarValue) -> DFResult<u8> {
        let result = match precision_scalar {
            ScalarValue::Int64(Some(precision)) => u8::try_from(*precision),
            ScalarValue::Int32(Some(precision)) => u8::try_from(*precision),
            ScalarValue::Int16(Some(precision)) => u8::try_from(*precision),
            ScalarValue::Int8(Some(precision)) => u8::try_from(*precision),
            ScalarValue::UInt64(Some(precision)) => u8::try_from(*precision),
            ScalarValue::UInt32(Some(precision)) => u8::try_from(*precision),
            ScalarValue::UInt16(Some(precision)) => u8::try_from(*precision),
            ScalarValue::UInt8(Some(precision)) => Ok(*precision),
            _ => {
                return conv_errors::UnsupportedInputTypeSnafu {
                    data_type: precision_scalar.data_type(),
                }
                .fail()?;
            }
        };
        let Ok(precision) = result else {
            return conv_errors::InvalidPrecisionSnafu {
                precision: precision_scalar.clone(),
            }
            .fail()?;
        };
        if !(1..=38).contains(&precision) {
            return conv_errors::InvalidPrecisionSnafu {
                precision: precision_scalar.clone(),
            }
            .fail()?;
        }
        Ok(precision)
    }
    #[allow(clippy::as_conversions, clippy::cast_possible_wrap)]
    fn get_scale_checked(scale_scalar: &ScalarValue, precision: u8) -> DFResult<i8> {
        let result = match scale_scalar {
            ScalarValue::Int64(Some(scale)) => i8::try_from(*scale),
            ScalarValue::Int32(Some(scale)) => i8::try_from(*scale),
            ScalarValue::Int16(Some(scale)) => i8::try_from(*scale),
            ScalarValue::Int8(Some(scale)) => Ok(*scale),
            ScalarValue::UInt64(Some(scale)) => i8::try_from(*scale),
            ScalarValue::UInt32(Some(scale)) => i8::try_from(*scale),
            ScalarValue::UInt16(Some(scale)) => i8::try_from(*scale),
            ScalarValue::UInt8(Some(scale)) => i8::try_from(*scale),
            _ => {
                return conv_errors::UnsupportedInputTypeSnafu {
                    data_type: scale_scalar.data_type(),
                }
                .fail()?;
            }
        };
        let Ok(scale) = result else {
            return conv_errors::InvalidScaleSnafu {
                precision_minus_one: precision - 1,
                scale: scale_scalar.clone(),
            }
            .fail()?;
        };
        if !(0..=((precision - 1) as i8)).contains(&scale) {
            return conv_errors::InvalidScaleSnafu {
                precision_minus_one: precision - 1,
                scale: scale_scalar.clone(),
            }
            .fail()?;
        }
        Ok(scale)
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
        conv_errors::ReturnTypeFromArgsShouldBeCalledSnafu.fail()?
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        match args.arg_types.len() {
            0 => conv_errors::TooLittleArgumentsSnafu {
                got: 0usize,
                at_least: 1usize,
            }
            .fail()?,
            1 => {
                //Variant json null -> null, not only for try_to_decimal
                Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true))
            }
            2 => match &args.scalar_arguments[1] {
                Some(
                    ScalarValue::Utf8(..) | ScalarValue::Utf8View(..) | ScalarValue::LargeUtf8(..),
                ) => Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true)),
                Some(precision) => {
                    let precision = Self::get_precision_checked(precision)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(precision, 0), true))
                }
                None => {
                    conv_errors::NoInputArgumentOnPositionsSnafu { positions: vec![2] }.fail()?
                }
            },
            3 => match &args.scalar_arguments[1..=2] {
                [
                    Some(
                        ScalarValue::Utf8(..)
                        | ScalarValue::Utf8View(..)
                        | ScalarValue::LargeUtf8(..),
                    ),
                    Some(precision),
                ] => {
                    let precision = Self::get_precision_checked(precision)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(precision, 0), true))
                }
                [Some(precision), Some(scale)] => {
                    let precision = Self::get_precision_checked(precision)?;
                    let scale = Self::get_scale_checked(scale, precision)?;
                    Ok(ReturnInfo::new(
                        DataType::Decimal128(precision, scale),
                        true,
                    ))
                }
                [..] => conv_errors::NoInputArgumentOnPositionsSnafu {
                    positions: vec![1, 2],
                }
                .fail()?,
            },
            4 => match &args.scalar_arguments[2..=3] {
                [Some(precision), Some(scale)] => {
                    let precision = Self::get_precision_checked(precision)?;
                    let scale = Self::get_scale_checked(scale, precision)?;
                    Ok(ReturnInfo::new(
                        DataType::Decimal128(precision, scale),
                        true,
                    ))
                }
                [..] => conv_errors::NoInputArgumentOnPositionsSnafu {
                    positions: vec![3, 4],
                }
                .fail()?,
            },
            other => conv_errors::TooManyArgumentsSnafu {
                got: other,
                at_maximum: 4usize,
            }
            .fail()?,
        }
    }
    //TODO: formatting <format> second argument
    #[allow(clippy::unwrap_used, clippy::too_many_lines)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let DataType::Decimal128(precision, scale) = args.return_type else {
            return conv_errors::UnexpectedReturnTypeSnafu {
                got: args.return_type.clone(),
                expected: DataType::Decimal128(38, 0),
            }
            .fail()?;
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
            other => {
                let other_array = match other {
                    ColumnarValue::Array(array) => array,
                    ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
                };
                return conv_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other_array.data_type().clone(),
                    position: 2usize,
                }
                .fail()?;
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
                                .map(|opt| opt.map(ToString::to_string))
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
            other => {
                return conv_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.clone(),
                    position: 1usize,
                }
                .fail()?;
            }
        };
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
