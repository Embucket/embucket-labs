use super::errors as conv_errors;
use arrow_schema::DataType;
use chrono::{DateTime, NaiveDate};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::util::display::FormatOptions;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, TypeSignatureClass};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{Array, StringArray};
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use std::any::Any;

#[derive(Debug)]
pub struct ToDateFunc {
    signature: Signature,
    aliases: Vec<String>,
    try_mode: bool,
}

impl ToDateFunc {
    #[must_use]
    pub fn new(try_mode: bool) -> Self {
        let aliases = if try_mode {
            Vec::with_capacity(0)
        } else {
            vec!["date".to_string()]
        };
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::String(1),
                    TypeSignature::String(2),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Timestamp,
                    )]),
                ],
                Volatility::Immutable,
            ),
            aliases,
            try_mode,
        }
    }

    fn extract_format_arg(args: &[ColumnarValue]) -> DFResult<Option<&str>> {
        if args.len() > 1 {
            match &args[1] {
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(format))
                    | ScalarValue::Utf8View(Some(format))
                    | ScalarValue::LargeUtf8(Some(format)),
                ) => {
                    let format = match format.to_lowercase().as_str() {
                        "yyyy-mm-dd" => Some("%Y-%m-%d"),
                        "mm/dd/yyyy" => Some("%m/%d/%Y"),
                        "dd/mm/yyyy" => Some("%d/%m/%Y"),
                        "yyyy.mm.dd" => Some("%Y.%m.%d"),
                        "dd-mon-yyyy" => Some("%e-%b-%Y"),
                        "dd-mmmm-yyyy" => Some("%e-%B-%Y"),
                        "auto" => None,
                        _ => {
                            return conv_errors::UnsupportedFormatSnafu {
                                format,
                                expected: "yyyy-mm-dd, yyyy.mm.dd, mm/dd/yyyy, dd/mm/yyyy, dd-mon-yyyy, dd-mmmm-yyyy & auto"
                                    .to_string(),
                            }
                            .fail()?;
                        }
                    };
                    Ok(format)
                }
                other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.data_type(),
                    position: 2usize,
                }
                .fail()?,
            }
        } else {
            Ok(None)
        }
    }

    fn parse_truncated_checked(str: &str) -> Option<i64> {
        let len = str.len();
        let truncated_str = if 19 < len {
            &str[0..(11 + (len - 20))]
        } else if 10 < len {
            &str[0..(8 + (len - 11) % 3)]
        } else {
            str
        };
        truncated_str.parse::<i64>().ok()
    }
}

impl ScalarUDFImpl for ToDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.try_mode {
            "try_to_date"
        } else {
            "to_date"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        match arg_types.len() {
            0 => conv_errors::NotEnoughArgumentsSnafu {
                got: 0usize,
                at_least: 1usize,
            }
            .fail()?,
            1 | 2 => Ok(DataType::Date32),
            n => conv_errors::TooManyArgumentsSnafu {
                got: n,
                at_maximum: 2usize,
            }
            .fail()?,
        }
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        //Already checked that it's at least > 0
        let expr = &args.args[0];
        let array = match expr {
            ColumnarValue::Array(array) => array,
            //Can't fail (shouldn't)
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        let cast_options = CastOptions {
            safe: self.try_mode,
            format_options: FormatOptions::default(),
        };

        let array = match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let string_array: &StringArray = array.as_any().downcast_ref().unwrap();

                let prepared_values: Vec<Option<String>> =
                    match Self::extract_format_arg(&args.args)? {
                        None => string_array
                            .iter()
                            .map(|opt| {
                                opt.map(|str| {
                                    if let Some(index) = str.find('T') {
                                        str[..index].to_string()
                                    } else if let Some(date_str) = str.split_whitespace().next() {
                                        date_str.to_string()
                                    } else if let Ok(date) =
                                        NaiveDate::parse_from_str(str, "%e-%b-%Y")
                                    {
                                        date.format("%Y-%m-%d").to_string()
                                    } else if let Ok(date) =
                                        NaiveDate::parse_from_str(str, "%e-%B-%Y")
                                    {
                                        date.format("%Y-%m-%d").to_string()
                                    } else if let Ok(date) =
                                        NaiveDate::parse_from_str(str, "%Y.%m.%d")
                                    {
                                        date.format("%Y-%m-%d").to_string()
                                    } else if let Ok(date) =
                                        NaiveDate::parse_from_str(str, "%m/%d/%Y")
                                    {
                                        date.format("%Y-%m-%d").to_string()
                                    } else if let Some(timestamp) =
                                        Self::parse_truncated_checked(str)
                                    {
                                        DateTime::from_timestamp(timestamp, 0).map_or_else(
                                            || str.to_string(),
                                            |date_time| {
                                                date_time
                                                    .date_naive()
                                                    .format("%Y-%m-%d")
                                                    .to_string()
                                            },
                                        )
                                    } else {
                                        str.to_string()
                                    }
                                })
                            })
                            .collect(),
                        Some(format) => string_array
                            .iter()
                            .map(|opt| {
                                opt.map(|str| {
                                    NaiveDate::parse_from_str(str, format).map_or_else(
                                        |_| str.to_string(),
                                        |date| date.format("%Y-%m-%d").to_string(),
                                    )
                                })
                            })
                            .collect(),
                    };

                let prepared_array = StringArray::from(prepared_values);

                cast_with_options(&prepared_array, &DataType::Date32, &cast_options)?
            }
            DataType::Timestamp(_, _) => {
                cast_with_options(array, &DataType::Date32, &cast_options)?
            }
            other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                data_type: other.clone(),
                position: 1usize,
            }
            .fail()?,
        };

        Ok(ColumnarValue::Array(array))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
