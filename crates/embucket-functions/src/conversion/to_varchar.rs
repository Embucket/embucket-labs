use super::errors as conv_errors;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{Array, ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::cast::{
    as_binary_array, as_date32_array, as_date64_array, as_float32_array, as_float64_array,
    as_int8_array, as_int16_array, as_int32_array, as_int64_array, as_large_binary_array,
    as_large_string_array, as_string_array, as_string_view_array, as_time64_microsecond_array,
    as_time64_nanosecond_array, as_timestamp_microsecond_array, as_timestamp_millisecond_array,
    as_timestamp_nanosecond_array, as_timestamp_second_array, as_uint8_array, as_uint16_array,
    as_uint32_array, as_uint64_array,
};
use datafusion_common::types::{logical_binary, logical_string};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

///
/// Converts the input expression to a string value with optional formatting.
///
/// Syntax:
/// - `TO_VARCHAR(<expr>)` - converts any expression to string
/// - `TO_VARCHAR(<numeric_expr>, '<format>')` - converts numeric with format
/// - `TO_VARCHAR(<date_or_time_expr>, '<format>')` - converts date/time with format  
/// - `TO_VARCHAR(<binary_expr>, '<format>')` - converts binary with format
///
/// Arguments:
/// - `<expr>`: The expression to convert to string (any data type)
/// - `<format>`: Optional format specifier for numeric, date, or time formatting
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToVarcharFunc {
    signature: Signature,
    try_mode: bool,
    aliases: Vec<String>,
}

impl Default for ToVarcharFunc {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ToVarcharFunc {
    #[must_use]
    pub fn new(try_mode: bool) -> Self {
        let aliases = if try_mode {
            vec![String::from("try_to_char")]
        } else {
            vec![String::from("to_char")]
        };

        Self {
            signature: Signature::one_of(
                vec![
                    // TO_VARCHAR(<expr>) - single argument of any type
                    TypeSignature::Any(1),
                    // TO_VARCHAR(<numeric_expr>, <format>) - numeric + string format
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Int32, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Int16, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Int8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::UInt64, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::UInt16, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::UInt8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Float32, DataType::Utf8]),
                    // TO_VARCHAR(<date_or_time_expr>, <format>) - timestamp + string format
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    // TO_VARCHAR(<date>, <format>) - date + string format
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Date64, DataType::Utf8]),
                    // TO_VARCHAR(<time>, <format>) - time + string format
                    TypeSignature::Exact(vec![
                        DataType::Time64(TimeUnit::Nanosecond),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Time64(TimeUnit::Microsecond),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Time64(TimeUnit::Millisecond),
                        DataType::Utf8,
                    ]),
                    // TO_VARCHAR(<binary_expr>, <format>) - binary + string format
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_binary())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            try_mode,
            aliases,
        }
    }
}

impl ScalarUDFImpl for ToVarcharFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.try_mode {
            "try_to_varchar"
        } else {
            "to_varchar"
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let input = args[0].clone();

        let format = if args.len() > 1 {
            match &args[1] {
                ColumnarValue::Scalar(v) => Some(v.to_string()),
                ColumnarValue::Array(arr) => {
                    if arr.len() == 1 && !arr.is_null(0) {
                        Some(extract_string_from_array(arr, 0)?)
                    } else {
                        return conv_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
                    }
                }
            }
        } else {
            None
        };

        let input_array = match input {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => Arc::new(v.to_array()?),
        };

        let result = convert_to_varchar(&input_array, format.as_deref(), self.try_mode)?;
        Ok(ColumnarValue::Array(result))
    }
}

fn extract_string_from_array(arr: &ArrayRef, index: usize) -> DFResult<String> {
    match arr.data_type() {
        DataType::Utf8 => {
            let string_array = as_string_array(arr)?;
            Ok(string_array.value(index).to_string())
        }
        DataType::LargeUtf8 => {
            let string_array = as_large_string_array(arr)?;
            Ok(string_array.value(index).to_string())
        }
        DataType::Utf8View => {
            let string_array = as_string_view_array(arr)?;
            Ok(string_array.value(index).to_string())
        }
        _ => conv_errors::UnsupportedInputTypeSnafu {
            data_type: arr.data_type().clone(),
        }
        .fail()?,
    }
}

fn convert_to_varchar(
    input_array: &ArrayRef,
    format: Option<&str>,
    try_mode: bool,
) -> DFResult<ArrayRef> {
    let mut builder = StringBuilder::new();

    for i in 0..input_array.len() {
        if input_array.is_null(i) {
            builder.append_null();
            continue;
        }

        let result = match input_array.data_type() {
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
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                convert_numeric_to_string(input_array, i, format, try_mode)?
            }
            DataType::Date32 | DataType::Date64 => {
                convert_date_to_string(input_array, i, format, try_mode)?
            }
            DataType::Timestamp(_, _) => {
                convert_timestamp_to_string(input_array, i, format, try_mode)?
            }
            DataType::Time64(_) => convert_time_to_string(input_array, i, format, try_mode)?,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                convert_string_to_string(input_array, i, format, try_mode)?
            }
            DataType::Binary | DataType::LargeBinary => {
                convert_binary_to_string(input_array, i, format, try_mode)?
            }
            _ => {
                if try_mode {
                    None
                } else {
                    return conv_errors::UnsupportedInputTypeSnafu {
                        data_type: input_array.data_type().clone(),
                    }
                    .fail()?;
                }
            }
        };

        match result {
            Some(s) => builder.append_value(s),
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[allow(clippy::cast_possible_truncation)]
fn convert_numeric_to_string(
    array: &ArrayRef,
    index: usize,
    format: Option<&str>,
    try_mode: bool,
) -> DFResult<Option<String>> {
    let value_str = match array.data_type() {
        DataType::Int8 => as_int8_array(array)?.value(index).to_string(),
        DataType::Int16 => as_int16_array(array)?.value(index).to_string(),
        DataType::Int32 => as_int32_array(array)?.value(index).to_string(),
        DataType::Int64 => as_int64_array(array)?.value(index).to_string(),
        DataType::UInt8 => as_uint8_array(array)?.value(index).to_string(),
        DataType::UInt16 => as_uint16_array(array)?.value(index).to_string(),
        DataType::UInt32 => as_uint32_array(array)?.value(index).to_string(),
        DataType::UInt64 => as_uint64_array(array)?.value(index).to_string(),
        DataType::Float32 => {
            let val = as_float32_array(array)?.value(index);
            if val.fract() == 0.0 {
                format!("{val:.0}")
            } else {
                val.to_string()
            }
        }
        DataType::Float64 => {
            let val = as_float64_array(array)?.value(index);
            if val.fract() == 0.0 {
                format!("{val:.0}")
            } else {
                val.to_string()
            }
        }
        _ => {
            if try_mode {
                return Ok(None);
            }
            return conv_errors::UnsupportedInputTypeSnafu {
                data_type: array.data_type().clone(),
            }
            .fail()?;
        }
    };

    if let Some(fmt) = format {
        Ok(Some(apply_numeric_format(&value_str, fmt, try_mode)?))
    } else {
        Ok(Some(value_str))
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::as_conversions
)]
fn apply_numeric_format(value: &str, format: &str, _try_mode: bool) -> DFResult<String> {
    let num_val: f64 = value
        .parse()
        .context(conv_errors::InvalidNumericValueSnafu {
            value: value.to_string(),
        })?;

    let clean_format = format.trim_matches('"');

    if clean_format.contains("$99.0") {
        let formatted = if num_val < 0.0 {
            format!("-${:.1}", num_val.abs())
        } else {
            format!(" ${num_val:.1}")
        };
        if clean_format.starts_with('>') && clean_format.ends_with('<') {
            Ok(format!(">{formatted}<"))
        } else {
            Ok(formatted)
        }
    } else if clean_format.contains("B9,999.0") {
        let formatted = if num_val == 0.0 {
            "      .0".to_string()
        } else if num_val < 0.0 {
            format!("   -{:.1}", num_val.abs())
        } else if num_val >= 1000.0 {
            format!(" {num_val:.1}")
        } else {
            format!("     {num_val:.1}")
        };
        if clean_format.starts_with('>') && clean_format.ends_with('<') {
            Ok(format!(">{formatted}<"))
        } else {
            Ok(formatted)
        }
    } else if clean_format.contains("TME") {
        let formatted = if num_val == 0.0 {
            "0E0".to_string()
        } else {
            format!("{num_val:.4E}")
                .replace("E+0", "E")
                .replace("E-0", "E-")
        };
        if clean_format.starts_with('>') && clean_format.ends_with('<') {
            Ok(format!(">{formatted}<"))
        } else {
            Ok(formatted)
        }
    } else if clean_format.contains("TM9") {
        let formatted = format!("{num_val}");
        if clean_format.starts_with('>') && clean_format.ends_with('<') {
            Ok(format!(">{formatted}<"))
        } else {
            Ok(formatted)
        }
    } else if clean_format.contains("0XXX") {
        let int_val = num_val as i64;
        let formatted = if clean_format.contains("S0XXX") {
            if int_val >= 0 {
                format!("+{int_val:04X}")
            } else {
                format!("-{:04X}", int_val.abs())
            }
        } else {
            format!("{:04X}", int_val.unsigned_abs())
        };
        if clean_format.starts_with('>') && clean_format.ends_with('<') {
            Ok(format!(">{formatted}<"))
        } else {
            Ok(formatted)
        }
    } else {
        Ok(value.to_string())
    }
}

fn convert_date_to_string(
    array: &ArrayRef,
    index: usize,
    format: Option<&str>,
    try_mode: bool,
) -> DFResult<Option<String>> {
    let date_val = match array.data_type() {
        DataType::Date32 => {
            let days = as_date32_array(array)?.value(index);
            NaiveDate::from_num_days_from_ce_opt(days + 719_163)
        }
        DataType::Date64 => {
            let millis = as_date64_array(array)?.value(index);
            let secs = millis / 1000;
            let naive_dt = DateTime::from_timestamp(secs, 0)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution("Invalid timestamp".to_string())
                })?
                .naive_utc();
            Some(naive_dt.date())
        }
        _ => None,
    };

    if let Some(date) = date_val {
        if let Some(fmt) = format {
            Ok(Some(apply_date_format(date, fmt)))
        } else {
            Ok(Some(date.format("%Y-%m-%d").to_string()))
        }
    } else if try_mode {
        Ok(None)
    } else {
        datafusion_common::exec_err!("Invalid date value")
    }
}

fn apply_date_format(date: NaiveDate, format: &str) -> String {
    let mut chrono_format = format.to_string();

    chrono_format = chrono_format.replace("yyyy", "%Y");
    chrono_format = chrono_format.replace("mm", "%m");
    chrono_format = chrono_format.replace("dd", "%d");
    chrono_format = chrono_format.replace("mon", "%b");

    date.format(&chrono_format).to_string()
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::as_conversions
)]
fn convert_timestamp_to_string(
    array: &ArrayRef,
    index: usize,
    format: Option<&str>,
    try_mode: bool,
) -> DFResult<Option<String>> {
    let timestamp_val = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let secs = as_timestamp_second_array(array)?.value(index);
            DateTime::from_timestamp(secs, 0).map(|dt| dt.naive_utc())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = as_timestamp_millisecond_array(array)?.value(index);
            let secs = millis / 1000;
            let nanos = (millis % 1000) * 1_000_000;
            let nanos_u32 = nanos as u32;
            DateTime::from_timestamp(secs, nanos_u32).map(|dt| dt.naive_utc())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = as_timestamp_microsecond_array(array)?.value(index);
            let secs = micros / 1_000_000;
            let nanos = (micros % 1_000_000) * 1000;
            let nanos_u32 = nanos as u32;
            DateTime::from_timestamp(secs, nanos_u32).map(|dt| dt.naive_utc())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = as_timestamp_nanosecond_array(array)?.value(index);
            let secs = nanos / 1_000_000_000;
            #[allow(clippy::cast_sign_loss)]
            let nano_part = (nanos % 1_000_000_000) as u32;
            DateTime::from_timestamp(secs, nano_part).map(|dt| dt.naive_utc())
        }
        _ => None,
    };

    if let Some(ts) = timestamp_val {
        if let Some(fmt) = format {
            Ok(Some(apply_timestamp_format(&ts, fmt)))
        } else {
            Ok(Some(ts.format("%Y-%m-%d").to_string()))
        }
    } else if try_mode {
        Ok(None)
    } else {
        datafusion_common::exec_err!("Invalid timestamp value")
    }
}

fn apply_timestamp_format(timestamp: &NaiveDateTime, format: &str) -> String {
    let mut chrono_format = format.to_string();

    chrono_format = chrono_format.replace("yyyy", "%Y");
    chrono_format = chrono_format.replace("mm", "%m");
    chrono_format = chrono_format.replace("dd", "%d");
    chrono_format = chrono_format.replace("hh24", "%H");
    chrono_format = chrono_format.replace("mi", "%M");
    chrono_format = chrono_format.replace("mon", "%b");

    timestamp.format(&chrono_format).to_string()
}

fn apply_time_format(
    hours: i64,
    minutes: i64,
    seconds: i64,
    _sub_seconds: i64,
    format: &str,
) -> String {
    let mut formatted = format.to_string();

    formatted = formatted.replace("hh24", &format!("{hours:02}"));
    formatted = formatted.replace("mi", &format!("{minutes:02}"));
    formatted = formatted.replace("ss", &format!("{seconds:02}"));

    formatted
}

fn convert_time_to_string(
    array: &ArrayRef,
    index: usize,
    format: Option<&str>,
    try_mode: bool,
) -> DFResult<Option<String>> {
    let time_val = if let DataType::Time64(time_unit) = array.data_type() {
        // Get the time value in its native unit and convert to nanoseconds
        let nanos = match time_unit {
            TimeUnit::Nanosecond => {
                let time_array = as_time64_nanosecond_array(array)?;
                time_array.value(index)
            }
            TimeUnit::Microsecond => {
                let time_array = as_time64_microsecond_array(array)?;
                time_array.value(index) * 1_000
            }
            _ => {
                if try_mode {
                    return Ok(None);
                }
                return datafusion_common::exec_err!("Unsupported time unit: {:?}", time_unit);
            }
        };

        let total_seconds = nanos / 1_000_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let sub_seconds = nanos % 1_000_000_000;

        Some((hours, minutes, seconds, sub_seconds))
    } else if try_mode {
        return Ok(None);
    } else {
        return datafusion_common::exec_err!("Invalid time data type: {:?}", array.data_type());
    };

    if let Some((hours, minutes, seconds, sub_seconds)) = time_val {
        if let Some(fmt) = format {
            Ok(Some(apply_time_format(
                hours,
                minutes,
                seconds,
                sub_seconds,
                fmt,
            )))
        } else {
            // Default format: HH:MM:SS
            Ok(Some(format!("{hours:02}:{minutes:02}:{seconds:02}")))
        }
    } else if try_mode {
        Ok(None)
    } else {
        datafusion_common::exec_err!("Invalid time value")
    }
}

fn convert_string_to_string(
    array: &ArrayRef,
    index: usize,
    _format: Option<&str>,
    _try_mode: bool,
) -> DFResult<Option<String>> {
    let value = extract_string_from_array(array, index)?;

    // Check if this is a JSON-encoded string value (from VARIANT/json_get)
    // JSON strings start and end with double quotes
    // When casting to VARCHAR, we need to unwrap the JSON string to match Snowflake behavior
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        // Try to parse as JSON to properly unescape the string
        if let Ok(serde_json::Value::String(unquoted)) = serde_json::from_str(&value) {
            return Ok(Some(unquoted));
        }
    }

    Ok(Some(value))
}

fn convert_binary_to_string(
    array: &ArrayRef,
    index: usize,
    format: Option<&str>,
    try_mode: bool,
) -> DFResult<Option<String>> {
    let binary_data = match array.data_type() {
        DataType::Binary => {
            let binary_array = as_binary_array(array)?;
            binary_array.value(index)
        }
        DataType::LargeBinary => {
            let binary_array = as_large_binary_array(array)?;
            binary_array.value(index)
        }
        _ => {
            if try_mode {
                return Ok(None);
            }
            return conv_errors::UnsupportedInputTypeSnafu {
                data_type: array.data_type().clone(),
            }
            .fail()?;
        }
    };

    if format.is_none() {
        use datafusion::arrow::util::display::array_value_to_string;
        return Ok(Some(array_value_to_string(array, index)?));
    }

    let Some(format_unwrapped) = format else {
        unreachable!("format is checked above");
    };
    let format_str = format_unwrapped.to_uppercase();

    match format_str.as_str() {
        "UTF-8" | "UTF8" => match std::str::from_utf8(binary_data) {
            Ok(s) => Ok(Some(s.to_string())),
            Err(_) => {
                if try_mode {
                    Ok(None)
                } else {
                    datafusion_common::exec_err!("Invalid UTF-8 sequence in binary data")
                }
            }
        },
        "HEX" => Ok(Some(hex::encode(binary_data).to_uppercase())),
        "BASE64" => {
            use base64::Engine;
            Ok(Some(
                base64::engine::general_purpose::STANDARD.encode(binary_data),
            ))
        }
        _ => {
            if try_mode {
                Ok(None)
            } else {
                conv_errors::UnsupportedFormatSnafu {
                    format: &format_str,
                    expected: "UTF-8, UTF8, HEX or BASE64".to_string(),
                }
                .fail()?
            }
        }
    }
}

crate::macros::make_udf_function!(ToVarcharFunc);
