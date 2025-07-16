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
use snafu::prelude::*;
use std::any::Any;
use std::fmt::Debug;
use std::num::TryFromIntError;
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
    /// Tries to convert a scalar to the target integer type
    fn try_convert_scalar<T>(scalar: &ScalarValue) -> Result<T, conv_errors::Error>
    where
        T: TryFrom<i128, Error = TryFromIntError>
            + TryFrom<i64, Error = TryFromIntError>
            + TryFrom<u64, Error = TryFromIntError>
            + Copy,
    {
        match scalar {
            ScalarValue::Int64(Some(v)) => {
                T::try_from(*v).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::Int32(Some(v)) => {
                T::try_from(i64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::Int16(Some(v)) => {
                T::try_from(i64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::Int8(Some(v)) => {
                T::try_from(i64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::UInt64(Some(v)) => {
                T::try_from(*v).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::UInt32(Some(v)) => {
                T::try_from(u64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::UInt16(Some(v)) => {
                T::try_from(u64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::UInt8(Some(v)) => {
                T::try_from(u64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::Decimal128(Some(v), ..) => {
                T::try_from(*v).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            _ => conv_errors::UnsupportedInputTypeSnafu {
                data_type: scalar.data_type(),
            }
            .fail(),
        }
    }
    fn get_precision_checked(precision_scalar: &ScalarValue) -> DFResult<u8> {
        let precision: u8 = Self::try_convert_scalar(precision_scalar)?;
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
        let scale: i8 = Self::try_convert_scalar(scale_scalar)?;
        if !(0..=((precision - 1) as i8)).contains(&scale) {
            return conv_errors::InvalidScaleSnafu {
                precision_minus_one: precision - 1,
                scale: scale_scalar.clone(),
            }
            .fail()?;
        }
        Ok(scale)
    }

    fn extract_format_arg(args: &[ColumnarValue]) -> DFResult<Option<&str>> {
        if args.len() > 1 {
            match &args[1] {
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(str))
                    | ScalarValue::Utf8View(Some(str))
                    | ScalarValue::LargeUtf8(Some(str)),
                ) => Ok(Some(str.as_str())),
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
                    | ScalarValue::Float32(Some(_))
                    | ScalarValue::Decimal128(..),
                ) => Ok(None),
                other => {
                    let other_array = match other {
                        ColumnarValue::Array(array) => array,
                        ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
                    };
                    conv_errors::UnsupportedInputTypeWithPositionSnafu {
                        data_type: other_array.data_type().clone(),
                        position: 2usize,
                    }
                    .fail()?
                }
            }
        } else {
            Ok(None)
        }
    }
    fn apply_formatting_if_needed(array: &StringArray, format: &str) -> Vec<Option<String>> {
        let values: Vec<Option<String>> = array
            .into_iter()
            .map(|opt| opt.map(|str| str.replace(' ', "")))
            .collect();

        let values = if format.starts_with('$') {
            values
                .into_iter()
                .map(|opt| {
                    opt.map(|str| {
                        str.strip_prefix('$')
                            .map_or_else(|| str.to_string(), std::string::ToString::to_string)
                    })
                })
                .collect()
        } else {
            values
        };

        let values: Vec<_> = if format.contains(',') {
            values
                .into_iter()
                .map(|opt| opt.as_ref().map(|str| str.replace(',', "")))
                .collect()
        } else {
            values
        };

        values
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
        use ScalarValue::{LargeUtf8, Utf8, Utf8View};

        match args.arg_types.len() {
            0 => conv_errors::TooLittleArgumentsSnafu {
                got: 0usize,
                at_least: 1usize,
            }
            .fail()?,
            1 => Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true)),
            2 => match &args.scalar_arguments[1] {
                Some(Utf8(..) | Utf8View(..) | LargeUtf8(..)) => {
                    Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true))
                }
                Some(precision) => {
                    let p = Self::get_precision_checked(precision)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(p, 0), true))
                }
                None => {
                    conv_errors::NoInputArgumentOnPositionsSnafu { positions: vec![2] }.fail()?
                }
            },
            3 => match (&args.scalar_arguments[1], &args.scalar_arguments[2]) {
                (Some(Utf8(..) | Utf8View(..) | LargeUtf8(..)), Some(precision)) => {
                    let p = Self::get_precision_checked(precision)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(p, 0), true))
                }
                (Some(precision), Some(scale)) => {
                    let p = Self::get_precision_checked(precision)?;
                    let s = Self::get_scale_checked(scale, p)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(p, s), true))
                }
                _ => conv_errors::NoInputArgumentOnPositionsSnafu {
                    positions: vec![1, 2],
                }
                .fail()?,
            },
            4 => match (&args.scalar_arguments[2], &args.scalar_arguments[3]) {
                (Some(precision), Some(scale)) => {
                    let p = Self::get_precision_checked(precision)?;
                    let s = Self::get_scale_checked(scale, p)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(p, s), true))
                }
                _ => conv_errors::NoInputArgumentOnPositionsSnafu {
                    positions: vec![3, 4],
                }
                .fail()?,
            },
            n => conv_errors::TooManyArgumentsSnafu {
                got: n,
                at_maximum: 4usize,
            }
            .fail()?,
        }
    }
    //TODO: formatting <format> second argument
    #[allow(clippy::unwrap_used)]
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

        let format = Self::extract_format_arg(&args.args)?;

        //TODO: should we have type info as before, good datapoint to think about on other types, functions, etc
        let cast_options = CastOptions {
            safe: self.r#try,
            format_options: FormatOptions::default(),
        };
        //TODO: override NULL formatting is not working, expected? Visitor somewhere?
        // .with_null("NULL")
        // .with_types_info(false);

        let result_array = match array.data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                let array = match format {
                    Some(format) => {
                        //TODO: needs logic for binary string with binary formatting and variant types
                        let array: &StringArray = array.as_any().downcast_ref().unwrap();

                        let values = Self::apply_formatting_if_needed(array, format);

                        Arc::new(StringArray::from(values))
                    }
                    None => array.clone(),
                };

                cast_with_options(
                    &array,
                    &DataType::Decimal128(*precision, *scale),
                    &cast_options,
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
            | DataType::Float64
            | DataType::Decimal128(..) => cast_with_options(
                array,
                &DataType::Decimal128(*precision, *scale),
                &cast_options,
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
