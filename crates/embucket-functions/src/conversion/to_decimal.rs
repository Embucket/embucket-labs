use super::errors as conv_errors;
use datafusion::arrow::array::Decimal128Array;
use datafusion::arrow::compute::{cast, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{Field, FieldRef};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::arrow::array::{Array, ArrayRef, StringArray};
use datafusion_common::arrow::compute::CastOptions;
use datafusion_common::arrow::util::display::FormatOptions;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use snafu::prelude::*;
use std::any::Any;
use std::fmt::Debug;
use std::num::TryFromIntError;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
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
            try_mode,
        }
    }
    /// Tries to convert a scalar to the target integer type
    fn try_convert_scalar_to_integer<T>(scalar: &ScalarValue) -> Result<T, conv_errors::Error>
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
                    scalar @ (ScalarValue::Int64(Some(_))
                    | ScalarValue::Int32(Some(_))
                    | ScalarValue::Int16(Some(_))
                    | ScalarValue::Int8(Some(_))
                    | ScalarValue::UInt64(Some(_))
                    | ScalarValue::UInt32(Some(_))
                    | ScalarValue::UInt16(Some(_))
                    | ScalarValue::UInt8(Some(_))
                    | ScalarValue::Float64(Some(_))
                    | ScalarValue::Float32(Some(_))
                    | ScalarValue::Decimal128(..)),
                ) => {
                    //Should if `SELECT TRY_TO_DECIMAL(1, 1, 1, 1);`
                    if args.len() > 3 {
                        return conv_errors::UnsupportedInputTypeWithPositionSnafu {
                            data_type: scalar.data_type(),
                            position: 2usize,
                        }
                        .fail()?;
                    }
                    Ok(None)
                }
                other => {
                    let other_array = match other {
                        ColumnarValue::Array(array) => array,
                        //Can't fail (shouldn't)
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
    fn error_or_null_array(&self, num_rows: usize, error: DataFusionError) -> DFResult<ArrayRef> {
        if self.try_mode {
            Ok(Arc::new(Decimal128Array::new_null(num_rows)))
        } else {
            Err(error)
        }
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        use ScalarValue::{LargeUtf8, Utf8, Utf8View};

        match args.arg_fields.len() {
            0 => conv_errors::NotEnoughArgumentsSnafu {
                got: 0usize,
                at_least: 1usize,
            }
            .fail()?,
            1 => Ok(Arc::new(Field::new(
                self.name(),
                DataType::Decimal128(38, 0),
                true,
            ))),
            2 => match &args.scalar_arguments[1] {
                Some(Utf8(..) | Utf8View(..) | LargeUtf8(..)) => Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Decimal128(38, 0),
                    true,
                ))),
                Some(precision) => {
                    let p = Self::get_precision_checked(precision)?;
                    Ok(Arc::new(Field::new(
                        self.name(),
                        DataType::Decimal128(p, 0),
                        true,
                    )))
                }
                None => {
                    conv_errors::NoInputArgumentOnPositionsSnafu { positions: vec![2] }.fail()?
                }
            },
            3 => match (&args.scalar_arguments[1], &args.scalar_arguments[2]) {
                (Some(Utf8(..) | Utf8View(..) | LargeUtf8(..)), Some(precision)) => {
                    let p = Self::get_precision_checked(precision)?;
                    Ok(Arc::new(Field::new(
                        self.name(),
                        DataType::Decimal128(p, 0),
                        true,
                    )))
                }
                (Some(precision), Some(scale)) => {
                    let p = Self::get_precision_checked(precision)?;
                    let s = Self::get_scale_checked(scale, p)?;
                    Ok(Arc::new(Field::new(
                        self.name(),
                        DataType::Decimal128(p, s),
                        true,
                    )))
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
                    Ok(Arc::new(Field::new(
                        self.name(),
                        DataType::Decimal128(p, s),
                        true,
                    )))
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
        let DataType::Decimal128(precision, scale) = args.return_type() else {
            return Ok(ColumnarValue::Array(
                self.error_or_null_array(
                    args.number_rows,
                    conv_errors::UnexpectedReturnTypeSnafu {
                        got: args.return_type().clone(),
                        expected: DataType::Decimal128(38, 0),
                    }
                    .fail()?,
                )?,
            ));
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
            // TRUE -> 1, FALSE -> 0
            DataType::Boolean => {
                // Cast boolean to integer first if possible
                let casted = cast(array, &DataType::Int64)?;
                cast_with_options(
                    &casted,
                    &DataType::Decimal128(*precision, *scale),
                    &cast_options,
                )?
            }
            other => self.error_or_null_array(
                args.number_rows,
                conv_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.clone(),
                    position: 1usize,
                }
                .fail()?,
            )?,
        };
        Ok(ColumnarValue::Array(result_array))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
