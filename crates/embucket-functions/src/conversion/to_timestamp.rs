use crate::conversion::{Micro, Nano, TimestampBuilder};
use crate::conversion_errors::{
    ArgumentTwoNeedsToBeIntegerSnafu, ArgumentTwoNeedsToBeStringSnafu, CantAddLocalTimezoneSnafu,
    CantCastToSnafu, CantGetTimestampSnafu, CantParseTimestampSnafu, CantParseTimezoneSnafu,
    FailedToParseIntSnafu, InvalidDataTypeSnafu, InvalidValueForFunctionAtPositionTwoSnafu,
};
use crate::session_params::SessionParams;
use chrono::{DateTime, FixedOffset, Month, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, Decimal128Array, StringArray, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    new_null_array,
};
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::arrow::array::{
    ArrayRef, TimestampMicrosecondBuilder, TimestampSecondBuilder,
};
use datafusion_common::cast::{as_int64_array, as_string_array};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{
    ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::arrow::datatypes::{Field, FieldRef};
use regex::Regex;
use snafu::{OptionExt, ResultExt};
use std::any::Any;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

#[allow(clippy::unwrap_used)]
static RE_TIMEZONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(Z|[+-]\d{2}:?\d{2}|\b(?:CEST|CET|EEST|EET|EST|EDT|PST|PDT|MST|MDT|CST|CDT|GMT|UTC|MSD|JST)\b)$"
    ).unwrap()
});

#[allow(clippy::unwrap_used)]
static TIMESTAMP_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(\d{1,4})[-/](\d{1,2}|[A-Za-z]{3})[-/](\d{2,4})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d+))?)?)?(?:\s?(Z|[+-]\d{2}(?::?\d{2})?|[A-Za-z]{2,4}))?$"
    ).unwrap()
});

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestampFunc {
    signature: Signature,
    session_params: Arc<SessionParams>,
    name: String,
    try_mode: bool,
}

impl Default for ToTimestampFunc {
    fn default() -> Self {
        Self::new(
            false,
            "to_timestamp".to_string(),
            Arc::new(SessionParams::default()),
        )
    }
}

impl ToTimestampFunc {
    #[must_use]
    pub fn new(try_mode: bool, name: String, session_params: Arc<SessionParams>) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            session_params,
            name,
            try_mode,
        }
    }

    #[must_use]
    pub fn input_format(&self) -> String {
        self.session_params
            .get_property("timestamp_input_format")
            .unwrap_or_else(|| "auto".to_string())
    }

    #[must_use]
    pub fn timezone(&self) -> Option<Arc<str>> {
        let timezone = self
            .session_params
            .get_property("timezone")
            .unwrap_or_else(|| "America/Los_Angeles".to_string());

        match self.name.as_str() {
            "to_timestamp_ntz" | "try_to_timestamp_ntz" => None,
            "to_timestamp" | "try_to_timestamp" => {
                if self
                    .session_params
                    .get_property("timestamp_input_mapping")
                    .unwrap_or_else(|| "timestamp_ntz".to_string())
                    == "timestamp_ntz"
                {
                    return None;
                }
                Some(Arc::from(timezone))
            }
            _ => Some(Arc::from(timezone)),
        }
    }

    fn unit_for_string_input(&self, v: &str) -> TimeUnit {
        if try_parse_with_auto(v, self.try_mode, TimeUnit::Nanosecond).is_ok() {
            TimeUnit::Nanosecond
        } else if try_parse_with_auto(v, self.try_mode, TimeUnit::Microsecond).is_ok() {
            TimeUnit::Microsecond
        } else {
            TimeUnit::Nanosecond
        }
    }
}

impl ScalarUDFImpl for ToTimestampFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        internal_err!("return_type_from_args should be called")
    }

    #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        // prefer value-dependent resolution similar to prior logic
        if args.scalar_arguments.len() == 1 {
            if args.arg_fields[0].data_type().is_numeric() {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Timestamp(TimeUnit::Second, self.timezone()),
                    true,
                )));
            } else if let Some(ScalarValue::Utf8(Some(v))) = args.scalar_arguments[0] {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Timestamp(self.unit_for_string_input(v), self.timezone()),
                    true,
                )));
            }
        } else if args.scalar_arguments.len() == 2 {
            if args.arg_fields[0].data_type().is_numeric() {
                if let Some(v) = args.scalar_arguments[1] {
                    let scale = v.cast_to(&DataType::Int64)?;
                    let ScalarValue::Int64(Some(s)) = &scale else {
                        return ArgumentTwoNeedsToBeIntegerSnafu.fail()?;
                    };
                    let s = *s;
                    let unit = match s {
                        0 => TimeUnit::Second,
                        3 => TimeUnit::Millisecond,
                        6 => TimeUnit::Microsecond,
                        9 => TimeUnit::Nanosecond,
                        _ => return InvalidValueForFunctionAtPositionTwoSnafu.fail()?,
                    };
                    return Ok(Arc::new(Field::new(
                        self.name(),
                        DataType::Timestamp(unit, self.timezone()),
                        true,
                    )));
                }
            } else if let Some(ScalarValue::TimestampSecond(_, Some(tz))) = args.scalar_arguments[0]
            {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Timestamp(TimeUnit::Second, Some(tz.to_owned())),
                    true,
                )));
            } else if let Some(ScalarValue::TimestampMillisecond(_, Some(tz))) =
                args.scalar_arguments[0]
            {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, Some(tz.to_owned())),
                    true,
                )));
            } else if let Some(ScalarValue::TimestampMicrosecond(_, Some(tz))) =
                args.scalar_arguments[0]
            {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(tz.to_owned())),
                    true,
                )));
            } else if let Some(ScalarValue::TimestampNanosecond(_, Some(tz))) =
                args.scalar_arguments[0]
            {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.to_owned())),
                    true,
                )));
            }
        }

        // If the first argument is already a timestamp type, return it as is (with its timezone).
        if matches!(
            args.arg_fields[0].data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
                | DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
        ) {
            return Ok(Arc::new(Field::new(
                self.name(),
                args.arg_fields[0].data_type().clone(),
                true,
            )));
        }

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Timestamp(TimeUnit::Nanosecond, self.timezone()),
            true,
        )))
    }
    #[allow(
        clippy::cognitive_complexity,
        clippy::too_many_lines,
        clippy::cast_possible_wrap,
        clippy::as_conversions,
        clippy::cast_lossless,
        clippy::cast_possible_truncation,
        clippy::unwrap_used
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        if arr.data_type().is_null() {
            let arr = new_null_array(
                &DataType::Timestamp(TimeUnit::Nanosecond, self.timezone()),
                arr.len(),
            );
            Ok(ColumnarValue::Array(Arc::new(arr)))
        } else if arr.data_type().is_integer() || arr.data_type().is_floating() {
            let arr = cast_with_options(&arr, &DataType::Int64, &DEFAULT_CAST_OPTIONS)?;

            let arr = as_int64_array(&arr)?;
            let scale = if args.len() == 1 {
                0
            } else if let ColumnarValue::Scalar(v) = &args[1] {
                let scale = v.cast_to(&DataType::Int64)?;
                if let ScalarValue::Int64(Some(v)) = &scale {
                    *v
                } else {
                    return ArgumentTwoNeedsToBeIntegerSnafu.fail()?;
                }
            } else {
                0
            };

            let arr: ArrayRef = match scale {
                0 => {
                    let mut b = TimestampSecondBuilder::with_capacity(arr.len())
                        .with_timezone_opt(self.timezone());
                    for v in arr {
                        match v {
                            None => b.append_null(),
                            Some(v) => b.append_value(v),
                        }
                    }
                    Arc::new(b.finish())
                }
                3 => {
                    let mut b = TimestampMillisecondBuilder::with_capacity(arr.len())
                        .with_timezone_opt(self.timezone());
                    for v in arr {
                        match v {
                            None => b.append_null(),
                            Some(v) => b.append_value(v),
                        }
                    }
                    Arc::new(b.finish())
                }
                6 => {
                    let mut b = TimestampMicrosecondBuilder::with_capacity(arr.len())
                        .with_timezone_opt(self.timezone());
                    for v in arr {
                        match v {
                            None => b.append_null(),
                            Some(v) => b.append_value(v),
                        }
                    }
                    Arc::new(b.finish())
                }
                9 => {
                    let mut b = TimestampNanosecondBuilder::with_capacity(arr.len())
                        .with_timezone_opt(self.timezone());
                    for v in arr {
                        match v {
                            None => b.append_null(),
                            Some(v) => b.append_value(v),
                        }
                    }
                    Arc::new(b.finish())
                }
                _ => return InvalidValueForFunctionAtPositionTwoSnafu.fail()?,
            };
            Ok(ColumnarValue::Array(Arc::new(arr)))
        } else if matches!(arr.data_type(), DataType::Timestamp(_, _)) {
            if matches!(
                arr.data_type(),
                DataType::Timestamp(TimeUnit::Microsecond, _)
                    | DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
            ) {
                return Ok(ColumnarValue::Array(Arc::new(arr)));
            }

            let DataType::Timestamp(_, tz) = arr.data_type() else {
                InvalidDataTypeSnafu.fail()?
            };

            let tz = if let Some(tz) = tz {
                Some(tz.clone())
            } else {
                self.timezone()
            };

            let arr = cast_with_options(
                &arr,
                &DataType::Timestamp(TimeUnit::Nanosecond, tz),
                &DEFAULT_CAST_OPTIONS,
            )?;
            Ok(ColumnarValue::Array(Arc::new(arr)))
        } else if matches!(arr.data_type(), DataType::Date32 | DataType::Date64) {
            let arr = cast_with_options(
                &arr,
                &DataType::Timestamp(TimeUnit::Nanosecond, self.timezone()),
                &DEFAULT_CAST_OPTIONS,
            )?;
            Ok(ColumnarValue::Array(Arc::new(arr)))
        } else if matches!(arr.data_type(), DataType::Decimal128(_, _)) {
            let DataType::Decimal128(_, s) = arr.data_type() else {
                InvalidDataTypeSnafu.fail()?
            };

            parse_decimal(&arr, &args, self.timezone(), *s)
        } else if matches!(
            arr.data_type(),
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        ) {
            let format = if args.len() > 1 {
                let ColumnarValue::Scalar(v) = &args[1] else {
                    return ArgumentTwoNeedsToBeStringSnafu.fail()?;
                };

                let format = v.cast_to(&DataType::Utf8)?;
                let ScalarValue::Utf8(Some(v)) = &format else {
                    return ArgumentTwoNeedsToBeStringSnafu.fail()?;
                };

                convert_snowflake_format_to_chrono(v)
            } else if &self.input_format().to_ascii_lowercase() != "auto" {
                convert_snowflake_format_to_chrono(&self.input_format())
            } else {
                "auto".to_string()
            };

            let arr = cast_with_options(&arr, &DataType::Utf8, &DEFAULT_CAST_OPTIONS)?;
            let arr = as_string_array(&arr)?;

            // For string inputs the timestamp value may exceed the i64 range if always treated as nanoseconds,
            // so we first determine the appropriate time unit and then choose the corresponding builder.
            let unit = self.unit_for_string_input(arr.value(0));
            let array: ArrayRef = match unit {
                TimeUnit::Microsecond => build_timestamp_array_from_scalar_string::<Micro>(
                    arr,
                    &format,
                    self.timezone(),
                    self.try_mode,
                )?,
                _ => build_timestamp_array_from_scalar_string::<Nano>(
                    arr,
                    &format,
                    self.timezone(),
                    self.try_mode,
                )?,
            };
            Ok(ColumnarValue::Array(Arc::new(array)))
        } else {
            InvalidDataTypeSnafu.fail()?
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn build_timestamp_array_from_scalar_string<T: TimestampBuilder>(
    arr: &StringArray,
    format: &str,
    tz: Option<Arc<str>>,
    try_mode: bool,
) -> DFResult<ArrayRef> {
    let mut builder = T::new(arr.len(), tz.clone());
    for v in arr {
        match v {
            None => T::append_null(&mut builder),
            Some(s) if contains_only_digits(s) => {
                let i = s
                    .parse::<i64>()
                    .context(FailedToParseIntSnafu { value: s })?;
                let t = match determine_timestamp_scale(i) {
                    0 => i * 1_000_000_000,
                    3 => i * 1_000_000,
                    6 => i * 1_000,
                    9 => i,
                    _ => return CantParseTimestampSnafu.fail()?,
                };
                T::append_value(&mut builder, t);
            }
            Some(s) => {
                let s = remove_timezone(s);
                let parsed_opt = if format.eq_ignore_ascii_case("auto") {
                    try_parse_with_auto(&s, try_mode, T::unit())?
                } else {
                    try_parse_with_format(&s, format, try_mode)?
                };
                match parsed_opt {
                    Some(parsed) => {
                        let t = if let Some(tz) = &tz {
                            apply_timezone(parsed, tz, try_mode, T::unit())?
                        } else {
                            parsed
                        };
                        T::append_value(&mut builder, t);
                    }
                    None => T::append_null(&mut builder),
                }
            }
        }
    }
    Ok(T::finish(builder))
}

#[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
fn try_parse_with_regex(s: &str, unit: TimeUnit) -> Option<i64> {
    if let Some(caps) = TIMESTAMP_REGEX.captures(s) {
        let first = caps[1].parse().ok()?;
        let second_str = &caps[2];
        let second: u32 = match Month::from_str(second_str) {
            Ok(m) => m.number_from_month(),
            Err(_) => second_str.parse().ok()?,
        };
        let third = caps[3].parse().ok()?;

        let (year, month, day) = if first > 31 {
            // YYYY-MM-DD or YYYY/MM/DD
            (first, second, third)
        } else if third > 31 {
            // DD-MMM-YYYY
            (third, second, first)
        } else {
            return None;
        };
        let (mut hour, mut minute, mut second, mut nano) = (0, 0, 0, 0);

        if let Some(hour_match) = caps.get(4) {
            hour = hour_match.as_str().parse().ok()?;
            minute = caps[5].parse().ok()?;
            second = caps.get(6).map_or(0, |m| m.as_str().parse().unwrap_or(0));
            nano = caps.get(7).map_or(0, |m| {
                let frac_str = m.as_str();
                let padded = format!("{:0<9}", &frac_str[..std::cmp::min(9, frac_str.len())]);
                padded.parse().unwrap_or(0)
            });
        }
        let tz_opt = caps.get(8).map(|m| m.as_str().trim());

        let naive = NaiveDate::from_ymd_opt(year as i32, month, day)?
            .and_hms_nano_opt(hour, minute, second, nano)?;
        let dt = if let Some(tz_str) = tz_opt {
            if let Some(offset) = parse_timezone(tz_str) {
                naive.and_local_timezone(offset).single()?.to_utc()
            } else {
                naive.and_utc()
            }
        } else {
            naive.and_utc()
        };

        match unit {
            TimeUnit::Microsecond => Some(dt.timestamp_micros()),
            _ => dt.timestamp_nanos_opt(),
        }
    } else {
        None
    }
}

fn try_parse_with_auto(s: &str, try_mode: bool, unit: TimeUnit) -> DFResult<Option<i64>> {
    if let Ok(v) = string_to_timestamp_nanos(s) {
        return Ok(Some(v));
    }
    if let Some(ts) = try_parse_with_regex(s, unit) {
        return Ok(Some(ts));
    }
    if try_mode {
        Ok(None)
    } else {
        CantGetTimestampSnafu.fail()?
    }
}

fn try_parse_with_format(s: &str, format: &str, try_mode: bool) -> DFResult<Option<i64>> {
    match NaiveDateTime::parse_from_str(s, format) {
        Ok(v) => Ok(v.and_utc().timestamp_nanos_opt()),
        Err(_) if try_mode => Ok(None),
        Err(_) => CantGetTimestampSnafu.fail()?,
    }
}

fn apply_timezone(ts: i64, tz_str: &str, try_mode: bool, unit: TimeUnit) -> DFResult<i64> {
    let tz: Tz = tz_str.parse().map_err(|_| CantParseTimezoneSnafu.build())?;
    let dt = match unit {
        TimeUnit::Microsecond => {
            DateTime::from_timestamp_micros(ts).context(CantGetTimestampSnafu)?
        }
        _ => DateTime::from_timestamp_nanos(ts),
    };
    if let Some(local) = dt.naive_utc().and_local_timezone(tz).single() {
        Ok(local
            .naive_utc()
            .and_utc()
            .timestamp_nanos_opt()
            .context(CantGetTimestampSnafu)?)
    } else if try_mode {
        Ok(0)
    } else {
        CantAddLocalTimezoneSnafu.fail()?
    }
}

#[must_use]
pub fn parse_timezone(s: &str) -> Option<FixedOffset> {
    match s.to_uppercase().as_str() {
        "Z" | "UTC" | "GMT" => Some(FixedOffset::east_opt(0)?),
        "CET" => Some(FixedOffset::east_opt(3600)?),
        "CEST" => Some(FixedOffset::east_opt(7200)?),
        "EET" => Some(FixedOffset::east_opt(7200)?),
        "EEST" => Some(FixedOffset::east_opt(10800)?),
        "PST" => Some(FixedOffset::west_opt(8 * 3600)?),
        "PDT" => Some(FixedOffset::west_opt(7 * 3600)?),
        "EST" => Some(FixedOffset::west_opt(5 * 3600)?),
        "EDT" => Some(FixedOffset::west_opt(4 * 3600)?),
        "CST" => Some(FixedOffset::west_opt(6 * 3600)?),
        "CDT" => Some(FixedOffset::west_opt(5 * 3600)?),
        "MST" => Some(FixedOffset::west_opt(7 * 3600)?),
        "MDT" => Some(FixedOffset::west_opt(6 * 3600)?),
        "JST" => Some(FixedOffset::east_opt(9 * 3600)?),
        "MSD" => Some(FixedOffset::east_opt(14400)?),
        _ => {
            if let Ok(fixed) = FixedOffset::from_str(s) {
                return Some(fixed);
            }
            if let Ok(tz) = s.parse::<Tz>() {
                let now = Utc::now().naive_utc();
                return Some(tz.offset_from_utc_datetime(&now).fix());
            }
            None
        }
    }
}

#[allow(
    clippy::cast_possible_wrap,
    clippy::as_conversions,
    clippy::cast_lossless,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::needless_pass_by_value
)]
fn parse_decimal(
    arr: &ArrayRef,
    args: &[ColumnarValue],
    timezone: Option<Arc<str>>,
    s: i8,
) -> DFResult<ColumnarValue> {
    let s = 10_i128.pow(s as u32);
    let scale = if args.len() == 1 {
        0
    } else if let ColumnarValue::Scalar(v) = &args[1] {
        let scale = v.cast_to(&DataType::Int64)?;
        let ScalarValue::Int64(Some(v)) = &scale else {
            return ArgumentTwoNeedsToBeIntegerSnafu.fail()?;
        };

        *v
    } else {
        0
    };

    let arr = arr
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| CantCastToSnafu { v: "decimal128" }.build())?;
    let arr: ArrayRef = match scale {
        0 => {
            let mut b =
                TimestampSecondBuilder::with_capacity(arr.len()).with_timezone_opt(timezone);
            for v in arr {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value((v / s) as i64),
                }
            }
            Arc::new(b.finish())
        }
        3 => {
            let mut b =
                TimestampMillisecondBuilder::with_capacity(arr.len()).with_timezone_opt(timezone);
            for v in arr {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value((v / s) as i64),
                }
            }
            Arc::new(b.finish())
        }
        6 => {
            let mut b =
                TimestampMicrosecondBuilder::with_capacity(arr.len()).with_timezone_opt(timezone);
            for v in arr {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value((v / s) as i64),
                }
            }
            Arc::new(b.finish())
        }
        9 => {
            let mut b =
                TimestampNanosecondBuilder::with_capacity(arr.len()).with_timezone_opt(timezone);
            for v in arr {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value((v / s) as i64),
                }
            }
            Arc::new(b.finish())
        }
        _ => return InvalidValueForFunctionAtPositionTwoSnafu.fail()?,
    };
    if arr.len() == 1 {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}
fn remove_timezone(datetime_str: &str) -> String {
    if datetime_str.len() < 15 {
        return datetime_str.to_string();
    }
    if let Some(caps) = RE_TIMEZONE.find(datetime_str) {
        return datetime_str[0..caps.start()].trim().to_string();
    }
    datetime_str.to_string()
}

fn contains_only_digits(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_digit())
}

#[must_use]
pub const fn determine_timestamp_scale(value: i64) -> u8 {
    const MILLIS_PER_YEAR: i64 = 31_536_000_000;
    const MICROS_PER_YEAR: i64 = 31_536_000_000_000;
    const NANOS_PER_YEAR: i64 = 31_536_000_000_000_000;

    let abs_value = value.abs();

    if abs_value < MILLIS_PER_YEAR {
        0
    } else if abs_value < MICROS_PER_YEAR {
        3
    } else if abs_value < NANOS_PER_YEAR {
        6
    } else {
        9
    }
}

#[must_use]
pub fn convert_snowflake_format_to_chrono(snowflake_format: &str) -> String {
    let mut chrono_format = snowflake_format.to_string().to_lowercase();

    chrono_format = chrono_format.replace("yyyy", "%Y");
    chrono_format = chrono_format.replace("yy", "%y");

    chrono_format = chrono_format.replace("mm", "%m");
    chrono_format = chrono_format.replace("mon", "%b");
    chrono_format = chrono_format.replace("month", "%B");

    chrono_format = chrono_format.replace("dd", "%d");
    chrono_format = chrono_format.replace("dy", "%a");
    chrono_format = chrono_format.replace("day", "%A");

    chrono_format = chrono_format.replace("hh24", "%H");
    chrono_format = chrono_format.replace("hh", "%I");
    chrono_format = chrono_format.replace("am", "%P");
    chrono_format = chrono_format.replace("pm", "%P");

    chrono_format = chrono_format.replace("mi", "%M");

    chrono_format = chrono_format.replace("ss", "%S");

    chrono_format = chrono_format.replace(".ff9", "%.9f");
    chrono_format = chrono_format.replace(".ff6", "%.6f");
    chrono_format = chrono_format.replace(".ff3", "%.3f");
    chrono_format = chrono_format.replace(".ff", "%.f");

    chrono_format = chrono_format.replace("tzh:tzm", "%z");
    chrono_format = chrono_format.replace("tzhtzm", "%Z");
    // T, Z, and UTC are parsed case-insensitively.
    chrono_format = chrono_format.replace('t', "T");

    chrono_format
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::visitors::timestamp;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement;
    use datafusion_common::assert_batches_eq;
    use datafusion_common::config::ExtensionOptions;
    use datafusion_expr::ScalarUDF;

    fn session_params(
        input_format: Option<String>,
        tz: Option<String>,
    ) -> DFResult<Arc<SessionParams>> {
        let mut session_params = SessionParams::default();
        if let Some(input_format) = input_format {
            session_params.set("timestamp_input_format", &input_format)?;
        }
        if let Some(tz) = tz {
            session_params.set("timezone", &tz)?;
        }
        Ok(Arc::new(session_params))
    }

    #[tokio::test]
    async fn test_scale() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 1970-01-12T13:46:40   | 1970-01-01T00:16:40   | 1970-01-01T00:00:01  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scaled() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scaled_tz() -> DFResult<()> {
        let mut session_params = SessionParams::default();
        session_params.set("timestamp_input_format", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM")?;
        session_params.set("timezone", "America/Los_Angeles")?;
        session_params.set("timestamp_input_mapping", "tz")?;

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            Arc::new(session_params),
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------+---------------------------+---------------------------+---------------------------+",
                "| Scale in seconds          | Scale in milliseconds     | Scale in microseconds     | Scale in nanoseconds      |",
                "+---------------------------+---------------------------+---------------------------+---------------------------+",
                "| 2001-09-08T18:46:40-07:00 | 2001-09-08T18:46:40-07:00 | 2001-09-08T18:46:40-07:00 | 2001-09-08T18:46:40-07:00 |",
                "+---------------------------+---------------------------+---------------------------+---------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_decimal() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 1970-01-12T13:46:40   | 1970-01-01T00:16:40   | 1970-01-01T00:00:01  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_decimal_scaled() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_int_str() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP('1000000000') AS "Scale in seconds",
       TO_TIMESTAMP('1000000000000') AS "Scale in milliseconds",
       TO_TIMESTAMP('1000000000000000') AS "Scale in microseconds",
       TO_TIMESTAMP('1000000000000000000') AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_predefined_format() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = "SELECT to_timestamp('2021-03-02 15:55:18.539000') as ts";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+-------------------------+",
                "| ts                      |",
                "+-------------------------+",
                "| 2021-03-02T15:55:18.539 |",
                "+-------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_timezone() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("auto".to_string()), None)?,
        )));

        let sql = "SELECT to_timestamp('2020-09-08T13:42:29.190855+01:00') as a, to_timestamp('1970-01-01T00:00:00Z') as b";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+----------------------------+---------------------+",
                "| a                          | b                   |",
                "+----------------------------+---------------------+",
                "| 2020-09-08T13:42:29.190855 | 1970-01-01T00:00:00 |",
                "+----------------------------+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timezone_str() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_tz".to_string(),
            session_params(
                Some("auto".to_string()),
                Some("America/Los_Angeles".to_string()),
            )?,
        )));

        let sql = "SELECT to_timestamp_tz('2020-09-08T13:42:29.190855+01:00') as a, to_timestamp_tz('2024-04-05 01:02:03') as b";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+----------------------------------+---------------------------+",
                "| a                                | b                         |",
                "+----------------------------------+---------------------------+",
                "| 2020-09-08T13:42:29.190855-07:00 | 2024-04-05T01:02:03-07:00 |",
                "+----------------------------------+---------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_str_format() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("mm/dd/yyyy hh24:mi:ss".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP('04/05/2024 01:02:03', 'mm/dd/yyyy hh24:mi:ss') as "a",
       TO_TIMESTAMP('04/05/2024T01:02:03', 'mm/dd/yyyyThh24:mi:ss') as "b",
       TO_TIMESTAMP('04/05/2024 01:02:03') as "c"
       "#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+---------------------+---------------------+",
                "| a                   | b                   | c                   |",
                "+---------------------+---------------------+---------------------+",
                "| 2024-04-05T01:02:03 | 2024-04-05T01:02:03 | 2024-04-05T01:02:03 |",
                "+---------------------+---------------------+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_auto() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("auto".to_string()), None)?,
        )));

        let sql = "SELECT TO_TIMESTAMP('05-Mar-2025') as ts";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| ts                  |",
                "+---------------------+",
                "| 2025-03-05T00:00:00 |",
                "+---------------------+",
            ],
            &result
        );

        let sql = "SELECT TO_TIMESTAMP('05-Mar-2025T01:02:03') as ts";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| ts                  |",
                "+---------------------+",
                "| 2025-03-05T01:02:03 |",
                "+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_null() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("mm/dd/yyyy hh24:mi:ss".to_string()), None)?,
        )));

        let sql = "SELECT TO_TIMESTAMP(NULL) as a";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(&["+---+", "| a |", "+---+", "|   |", "+---+",], &result);
        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000::TIMESTAMP) as "a""#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| a                   |",
                "+---------------------+",
                "| 2001-09-09T01:46:40 |",
                "+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_date() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP('2022-01-01 11:30:00'::date) as "a""#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| a                   |",
                "+---------------------+",
                "| 2022-01-01T00:00:00 |",
                "+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timezone() -> DFResult<()> {
        let mut session_params = SessionParams::default();
        session_params.set("timestamp_input_format", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM")?;
        session_params.set("timezone", "America/Los_Angeles")?;
        session_params.set("timestamp_input_mapping", "tz")?;

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            Arc::new(session_params),
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000) as "a""#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------+",
                "| a                         |",
                "+---------------------------+",
                "| 2001-09-08T18:46:40-07:00 |",
                "+---------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_different_names() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_ntz".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_tz".to_string(),
            session_params(
                Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()),
                Some("America/Los_Angeles".to_string()),
            )?,
        )));

        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_ltz".to_string(),
            session_params(
                Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()),
                Some("America/Los_Angeles".to_string()),
            )?,
        )));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000) as "a",
       TO_TIMESTAMP_NTZ(1000000000) as "b",
       TO_TIMESTAMP_TZ(1000000000) as "c",
       TO_TIMESTAMP_LTZ(1000000000) as "d"
       "#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+---------------------+---------------------------+---------------------------+",
                "| a                   | b                   | c                         | d                         |",
                "+---------------------+---------------------+---------------------------+---------------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40 | 2001-09-08T18:46:40-07:00 | 2001-09-08T18:46:40-07:00 |",
                "+---------------------+---------------------+---------------------------+---------------------------+",
            ],
            &result
        );

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            Arc::new(SessionParams::default()),
        )));

        let sql = "SELECT
           TO_TIMESTAMP('2024-04-02 10:00') as a,
           TO_TIMESTAMP('2024-04-02 10:00:01') as b";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+---------------------+",
                "| a                   | b                   |",
                "+---------------------+---------------------+",
                "| 2024-04-02T10:00:00 | 2024-04-02T10:00:01 |",
                "+---------------------+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_try() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            true,
            "try_to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        let sql = "SELECT TRY_TO_TIMESTAMP('sdfsdf')";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------------------------------+",
                "| try_to_timestamp(Utf8(\"sdfsdf\")) |",
                "+----------------------------------+",
                "|                                  |",
                "+----------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_visitor() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_ntz".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_tz".to_string(),
            session_params(Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()), None)?,
        )));

        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new(
            false,
            "to_timestamp_ltz".to_string(),
            session_params(
                Some("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()),
                Some("America/Los_Angeles".to_string()),
            )?,
        )));

        let sql = "SELECT
        1000000000::TIMESTAMP as a,
        1000000000::TIMESTAMP_NTZ as b,
        1000000000::TIMESTAMP_TZ as c,
        1000000000::TIMESTAMP_LTZ as d,
         '2025-07-04 19:16:30+02:00'::TIMESTAMP_TZ as e";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            timestamp::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+---------------------+---------------------------+---------------------------+---------------------------+",
                "| a                   | b                   | c                         | d                         | e                         |",
                "+---------------------+---------------------+---------------------------+---------------------------+---------------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40 | 2001-09-08T18:46:40-07:00 | 2001-09-08T18:46:40-07:00 | 2025-07-04T19:16:30-07:00 |",
                "+---------------------+---------------------+---------------------------+---------------------------+---------------------------+",
            ],
            &result
        );
        Ok(())
    }
}
