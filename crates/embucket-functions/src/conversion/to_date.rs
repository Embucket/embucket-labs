use super::errors as conv_errors;
use arrow_schema::DataType;
use chrono::{DateTime, Datelike, NaiveDate};
use datafusion::arrow::array::Date32Array;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::util::display::FormatOptions;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, TypeSignatureClass};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{Array, StringArray};
use datafusion_common::cast::as_generic_string_array;
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use std::any::Any;
use std::sync::Arc;

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

const YYYY_MM_DD_FORMAT: &str = "%Y-%m-%d";
const MM_DD_YYYY_SLASH_FORMAT: &str = "%m/%d/%Y";
const YYYY_MM_DD_DOT_FORMAT: &str = "%Y.%m.%d";
const DD_MM_YYYY_SLASH_FORMAT: &str = "%d/%m/%Y";
const D_MON_YYYY_FORMAT: &str = "%e-%b-%Y";
const D_MMMM_YYYY_FORMAT: &str = "%e-%B-%Y";

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

    //TODO: rewrite to any format support
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
                let string_array: &StringArray = as_generic_string_array(array)?;

                let mut date32_array_builder = Date32Array::builder(string_array.len());

                match Self::extract_format_arg(&args.args)? {
                    None => string_array.iter().for_each(|opt| {
                        if let Some(str) = opt {
                            let str = str.trim();
                            let mut tokens = str.split_whitespace();
                            //TODO: better naming + checked_sub + error if not try_mode
                            let result = tokens.next().filter(|_| tokens.next().is_some());
                            if let Some(index) = str.find('T') {
                                date32_array_builder.append_option(
                                    NaiveDate::parse_from_str(&str[..index], YYYY_MM_DD_FORMAT)
                                        .ok()
                                        .map(|date| {
                                            date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
                                        }),
                                );
                            } else if let Some(date_str) = result {
                                date32_array_builder.append_option(
                                    NaiveDate::parse_from_str(date_str, YYYY_MM_DD_FORMAT)
                                        .ok()
                                        .map(|date| {
                                            date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
                                        }),
                                );
                            } else if let Ok(date) =
                                NaiveDate::parse_from_str(str, D_MON_YYYY_FORMAT)
                            {
                                date32_array_builder.append_value(
                                    date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE,
                                );
                            } else if let Ok(date) =
                                NaiveDate::parse_from_str(str, D_MMMM_YYYY_FORMAT)
                            {
                                date32_array_builder.append_value(
                                    date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE,
                                );
                            } else if let Ok(date) =
                                NaiveDate::parse_from_str(str, YYYY_MM_DD_DOT_FORMAT)
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
                            } else if let Ok(date) =
                                NaiveDate::parse_from_str(str, DD_MM_YYYY_SLASH_FORMAT)
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
                            } else {
                                date32_array_builder.append_null();
                            }
                        } else {
                            date32_array_builder.append_null();
                        }
                    }),
                    Some(format) => string_array.iter().for_each(|opt| {
                        if let Some(str) = opt {
                            date32_array_builder.append_option(
                                NaiveDate::parse_from_str(str, format)
                                    .ok()
                                    .map(|date| date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE),
                            );
                        } else {
                            date32_array_builder.append_null();
                        }
                    }),
                }

                Arc::new(date32_array_builder.finish())
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
