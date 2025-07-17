use super::errors as conv_errors;
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
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
    try_mode: bool,
}

impl ToDecimalFunc {
    #[must_use]
    pub fn new(try_mode: bool) -> Self {
        let aliases = if try_mode {
            vec![
                String::from("try_to_number"),
                String::from("try_to_numeric"),
            ]
        } else {
            vec![String::from("to_number"), String::from("to_numeric")]
        };
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases,
            try_mode,
        }
    }
    fn coerce_expr_data_type(expr: &DataType) -> DFResult<DataType> {
        match expr {
            DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..) => Ok(expr.clone()),
            other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                data_type: other.clone(),
                position: 1usize,
            }
            .fail()?,
        }
    }
    fn coerce_format_data_type(format: &DataType) -> DFResult<DataType> {
        match format {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(format.clone()),
            other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                data_type: other.clone(),
                position: 2usize,
            }
            .fail()?,
        }
    }
    fn coerce_precision_data_type(precision: &DataType, position: usize) -> DFResult<DataType> {
        match precision {
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
            | DataType::Decimal128(..) => {
                tracing::error!("precision: {:?}", precision);
                Ok(DataType::UInt8)
            }
            other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                data_type: other.clone(),
                position,
            }
            .fail()?,
        }
    }
    fn coerce_scale_data_type(scale: &DataType, position: usize) -> DFResult<DataType> {
        match scale {
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
            | DataType::Decimal128(..) => {
                tracing::error!("scale: {:?}", scale);
                Ok(DataType::Int8)
            }
            other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                data_type: other.clone(),
                position,
            }
            .fail()?,
        }
    }
    /// Tries to convert a scalar to the target integer type
    fn try_convert_scalar_to_integer<T>(scalar: &ScalarValue) -> Result<T, conv_errors::Error>
    where
        T: TryFrom<i64, Error = TryFromIntError> + TryFrom<u64, Error = TryFromIntError> + Copy,
    {
        match scalar {
            ScalarValue::Int8(Some(v)) => {
                T::try_from(i64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::UInt8(Some(v)) => {
                T::try_from(u64::from(*v)).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            ScalarValue::Int64(Some(v)) => {
                T::try_from(*v).context(conv_errors::InvalidIntegerConversionSnafu)
            }
            other => conv_errors::UnsupportedInputTypeSnafu {
                data_type: other.data_type(),
            }
            .fail()?,
        }
    }
    fn get_precision_checked(precision_scalar: &ScalarValue) -> DFResult<u8> {
        let precision: u8 = Self::try_convert_scalar_to_integer(precision_scalar)?;
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
        let scale: i8 = Self::try_convert_scalar_to_integer(scale_scalar)?;
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
                    scalar @ (ScalarValue::Int8(Some(_)) | ScalarValue::UInt8(Some(_))),
                ) => {
                    //Should error if `SELECT TRY_TO_DECIMAL(1, 1, 1, 1);`
                    if args.len() > 3 {
                        return conv_errors::UnsupportedInputTypeWithPositionSnafu {
                            data_type: scalar.data_type(),
                            position: 2usize,
                        }
                        .fail()?;
                    }
                    Ok(None)
                }
                //Already coerced the types before
                _ => unreachable!(),
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
        if self.try_mode {
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

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        match arg_types {
            [] => conv_errors::NotEnoughArgumentsSnafu {
                got: 0usize,
                at_least: 1usize,
            }
            .fail()?,
            [expr] => Ok(vec![Self::coerce_expr_data_type(expr)?]),
            [expr, second_arg] => {
                let second_arg = if let Ok(format) = Self::coerce_format_data_type(second_arg) {
                    format
                } else {
                    Self::coerce_precision_data_type(second_arg, 2)?
                };
                Ok(vec![Self::coerce_expr_data_type(expr)?, second_arg])
            }
            [expr, second_arg, third_arg] => {
                let (second_arg, third_arg) = match Self::coerce_format_data_type(second_arg) {
                    Ok(format) => (format, Self::coerce_precision_data_type(third_arg, 3)?),
                    Err(_) => (
                        Self::coerce_precision_data_type(second_arg, 2)?,
                        Self::coerce_scale_data_type(third_arg, 3)?,
                    ),
                };
                Ok(vec![
                    Self::coerce_expr_data_type(expr)?,
                    second_arg,
                    third_arg,
                ])
            }
            [expr, format, precision, scale] => Ok(vec![
                Self::coerce_expr_data_type(expr)?,
                Self::coerce_format_data_type(format)?,
                Self::coerce_precision_data_type(precision, 3)?,
                Self::coerce_scale_data_type(scale, 4)?,
            ]),
            [..] => conv_errors::TooManyArgumentsSnafu {
                got: arg_types.len(),
                at_maximum: 4usize,
            }
            .fail()?,
        }
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        use ScalarValue::{LargeUtf8, Utf8, Utf8View};

        match args.arg_types.len() {
            1 => Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true)),
            2 => match &args.scalar_arguments[1] {
                Some(Utf8(..) | Utf8View(..) | LargeUtf8(..)) => {
                    Ok(ReturnInfo::new(DataType::Decimal128(38, 0), true))
                }
                Some(precision) => {
                    let p = Self::get_precision_checked(precision)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(p, 0), true))
                }
                other => {
                    tracing::error!("{:?}", args.scalar_arguments);
                    if let Some(other) = other {
                        conv_errors::UnsupportedInputTypeSnafu {
                            data_type: other.data_type(),
                        }.fail()?
                    } else {
                        conv_errors::UnsupportedInputTypeSnafu {
                            data_type: DataType::Null,
                        }.fail()?
                    }
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
                other => {
                    tracing::error!("{:?}", args.scalar_arguments);
                    if let Some(other) = other.0 {
                        conv_errors::UnsupportedInputTypeSnafu {
                            data_type: other.data_type(),
                        }.fail()?
                    } else {
                        conv_errors::UnsupportedInputTypeSnafu {
                            data_type: DataType::Null,
                        }.fail()?
                    }
                }
            },
            4 => match (&args.scalar_arguments[2], &args.scalar_arguments[3]) {
                (Some(precision), Some(scale)) => {
                    let p = Self::get_precision_checked(precision)?;
                    let s = Self::get_scale_checked(scale, p)?;
                    Ok(ReturnInfo::new(DataType::Decimal128(p, s), true))
                }
                other => {
                    tracing::error!("{:?}", args.scalar_arguments);
                    if let Some(other) = other.0 {
                        conv_errors::UnsupportedInputTypeSnafu {
                            data_type: other.data_type(),
                        }.fail()?
                    } else {
                        conv_errors::UnsupportedInputTypeSnafu {
                            data_type: DataType::Null,
                        }.fail()?
                    }
                }
            },
            //Already checked size when coercing types
            _ => unreachable!(),
        }
    }
    //TODO: formatting <format> second argument
    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let DataType::Decimal128(precision, scale) = args.return_type else {
            //Can't fail (shouldn't)
            return conv_errors::UnexpectedReturnTypeSnafu {
                got: args.return_type.clone(),
                expected: DataType::Decimal128(38, 0),
            }
            .fail()?;
        };

        let expr = &args.args[0];
        let array = match expr {
            ColumnarValue::Array(array) => array,
            //Can't fail (shouldn't)
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        //Expected to error even with `try_`
        let format = Self::extract_format_arg(&args.args)?;

        let cast_options = CastOptions {
            safe: self.try_mode,
            format_options: FormatOptions::default(),
        };

        let result_array = match array.data_type() {
            //Only to apply formatting
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
            //Any other type is already coerced (checked at an analyzer step)
            _ => cast_with_options(
                &array,
                &DataType::Decimal128(*precision, *scale),
                &cast_options,
            )?,
        };
        Ok(ColumnarValue::Array(result_array))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
