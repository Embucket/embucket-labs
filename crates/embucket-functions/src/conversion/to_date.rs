use super::errors as conv_errors;
use arrow_schema::DataType;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::util::display::FormatOptions;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, TypeSignatureClass};
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use std::any::Any;
use std::str::FromStr;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use datafusion_common::arrow::array::{Array, StringArray};

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

    const fn extract_format_arg(&self, args: &[ColumnarValue]) -> Option<&str> {
        None
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

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
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

                let prepared_array: Vec<_> = string_array.iter().map(|opt| {
                    opt.map(|str| {
                        if str.contains("/") {
                            NaiveDate::parse_from_str(str, "%m/%d/%Y").unwrap().format("%Y-%m-%d").to_string()
                        } else if str.contains(".") {
                            NaiveDate::parse_from_str(str, "%Y.%m.%d").unwrap().format("%Y-%m-%d").to_string()
                        } else if str.contains("-") {
                            str.to_string()
                        } else {
                            let truncated_str = &str[0..str.len().min(10)];
                            let timestamp = truncated_str.parse::<i64>().unwrap();
                            DateTime::from_timestamp(timestamp, 0).unwrap_or_default().date_naive().format("%Y-%m-%d").to_string()
                        }
                    })
                }).collect();

                let prepared_array = StringArray::from(prepared_array);

                let format = self.extract_format_arg(&args.args);

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
