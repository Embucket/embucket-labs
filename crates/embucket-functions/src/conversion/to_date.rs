use super::errors as conv_errors;
use crate::utils::to_string_array;
use chrono::{DateTime, Datelike, NaiveDate};
use datafusion::arrow::array::Date32Array;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::FormatOptions;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, TypeSignatureClass};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{Array, ArrayRef, StringArray};
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use std::any::Any;
use std::ops::Add;
use std::sync::Arc;

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;
const YYYY_MM_DD_FORMAT: &str = "%Y-%m-%d";
const DD_MON_YYYY_FORMAT: &str = "%d-%B-%Y";
const MM_DD_YYYY_SLASH_FORMAT: &str = "%m/%d/%Y";

/// `TO_DATE`, `DATE` & `TRY_TO_DATE` function implementation
///
/// Converts an input expression to a date
/// For a VARCHAR expression, the result of converting the string to a date.
/// For a TIMESTAMP expression, the date from the timestamp.
/// For NULL input, the output is NULL.
///
/// Syntax: `TO_DATE(<expr> [, <format> ])`
///
/// Arguments:
/// - `<expr>`:
///   - String from which to extract a date. For example: '2024-01-31'.
///   - A TIMESTAMP expression. The DATE portion of the TIMESTAMP value is extracted.
///   - An expression that evaluates to a string containing an integer. For example: '15000000'.
///     Depending on the magnitude of the string, it can be interpreted as seconds, milliseconds, microseconds, or nanoseconds.
/// - Optional `<format>` date format specifier for `string_expr` or AUTO, which specifies that Snowflake automatically detects the format to use.
///
/// Example: `TO_DATE('2024-05-10')`
#[derive(Debug, PartialEq, Eq, Hash)]
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
                    TypeSignature::Exact(vec![DataType::Date32]),
                ],
                Volatility::Immutable,
            ),
            aliases,
            try_mode,
        }
    }
    fn extract_format_arg(args: &[ColumnarValue]) -> DFResult<Option<String>> {
        if args.len() > 1 {
            match &args[1] {
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(format))
                    | ScalarValue::Utf8View(Some(format))
                    | ScalarValue::LargeUtf8(Some(format)),
                ) => {
                    let format = format.trim().to_lowercase();
                    let format = if format.as_str() == "auto" {
                        None
                    } else {
                        Some(format)
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

    fn parse_to_chrono_format(format: &str) -> String {
        format
            .replace("yyyy", "%C%y")
            .replace("yy", "%Y")
            .replace("mon", "%B")
            .replace("mmmm", "%B")
            .replace("mm", "%m")
            .replace("dd", "%d")
    }

    fn format_missing_chrono_format(format: &str) -> (String, &str) {
        let format = format.to_string();
        if !format.contains("%Y") && !format.contains("%y") {
            (format.add("%Y"), "1970")
        } else if !format.contains("%m") && !format.contains("%B") {
            (format.add("%m"), "01")
        } else if !format.contains("%d") {
            (format.add("%d"), "01")
        } else {
            (format, "")
        }
    }

    fn format_date_str(str: &str) -> &str {
        if let Some(index) = str.trim().find('T').or_else(|| str.find(' ')) {
            &str[..index]
        } else {
            str
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

    fn to_date(
        &self,
        array: &dyn Array,
        args: &[ColumnarValue],
        cast_options: &CastOptions,
    ) -> DFResult<ArrayRef> {
        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let string_array: &StringArray = &to_string_array(array)?;

                let mut date32_array_builder = Date32Array::builder(string_array.len());

                let real_format = Self::extract_format_arg(args)?;

                match real_format {
                    None => {
                        for opt in string_array {
                            if let Some(str) = opt {
                                let str = Self::format_date_str(str);
                                if let Ok(date) = NaiveDate::parse_from_str(str, YYYY_MM_DD_FORMAT)
                                {
                                    date32_array_builder.append_value(
                                        date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE,
                                    );
                                } else if let Ok(date) =
                                    NaiveDate::parse_from_str(str, DD_MON_YYYY_FORMAT)
                                {
                                    date32_array_builder.append_value(
                                        date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE,
                                    );
                                } else if let Ok(date) =
                                    NaiveDate::parse_from_str(str, MM_DD_YYYY_SLASH_FORMAT)
                                {
                                    date32_array_builder.append_value(
                                        date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE,
                                    );
                                } else if let Some(timestamp) = Self::parse_truncated_checked(str) {
                                    date32_array_builder.append_option(
                                        DateTime::from_timestamp(timestamp, 0).map(|date_time| {
                                            date_time.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
                                        }),
                                    );
                                //if we can't parse it
                                } else if self.try_mode {
                                    date32_array_builder.append_null();
                                } else {
                                    return conv_errors::UnsupportedValueFormatSnafu { value: str.to_string(), formats: format!("{YYYY_MM_DD_FORMAT}, {DD_MON_YYYY_FORMAT} & {MM_DD_YYYY_SLASH_FORMAT}") }.fail()?;
                                }
                            //if the value was null
                            } else {
                                date32_array_builder.append_null();
                            }
                        }
                    }
                    Some(real_format) => {
                        let chrono_format = Self::parse_to_chrono_format(&real_format);
                        let (format, missing) = Self::format_missing_chrono_format(&chrono_format);
                        for opt in string_array {
                            if let Some(str) = opt {
                                if let Ok(date) = NaiveDate::parse_from_str(
                                    str.to_string().add(missing).as_str(),
                                    format.as_str(),
                                ) {
                                    let date = if date.year() < 70 {
                                        date.with_year(date.year() + 2000).map(|date| {
                                            date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
                                        })
                                    } else if date.year() < 100 {
                                        date.with_year(date.year() + 1900).map(|date| {
                                            date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
                                        })
                                    } else if date.year() < 1970 && self.try_mode {
                                        None
                                    } else if date.year() < 1970 && !self.try_mode {
                                        return conv_errors::UnsupportedValueFormatSnafu {
                                            value: str.to_string(),
                                            formats: real_format,
                                        }
                                        .fail()?;
                                    } else {
                                        Some(date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE)
                                    };
                                    date32_array_builder.append_option(date);
                                //if we can't parse it
                                } else if self.try_mode {
                                    date32_array_builder.append_null();
                                } else {
                                    return conv_errors::UnsupportedValueFormatSnafu {
                                        value: str.to_string(),
                                        formats: real_format,
                                    }
                                    .fail()?;
                                }
                            //if the value was null
                            } else {
                                date32_array_builder.append_null();
                            }
                        }
                    }
                }

                Ok(Arc::new(date32_array_builder.finish()))
            }
            DataType::Timestamp(_, _) | DataType::Date32 => {
                Ok(cast_with_options(array, &DataType::Date32, cast_options)?)
            }
            other => conv_errors::UnsupportedInputTypeWithPositionSnafu {
                data_type: other.clone(),
                position: 1usize,
            }
            .fail()?,
        }
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

        let array = self.to_date(array, &args.args, &cast_options)?;

        Ok(ColumnarValue::Array(array))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
