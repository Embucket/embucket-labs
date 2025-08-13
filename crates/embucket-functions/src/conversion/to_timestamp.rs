use chrono::{DateTime, NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, Decimal128Array, StringArray, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    new_null_array,
};
use datafusion::arrow::compute::kernels;
use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::arrow::array::{
    ArrayRef, TimestampMicrosecondBuilder, TimestampSecondBuilder,
};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{ScalarValue, internal_err};

use crate::conversion_errors::{
    ArgumentTwoNeedsToBeIntegerSnafu, ArgumentTwoNeedsToBeStringSnafu, CantAddLocalTimezoneSnafu,
    CantCastToSnafu, CantGetTimestampSnafu, CantParseTimestampSnafu, CantParseTimezoneSnafu,
    InvalidDataTypeSnafu, InvalidValueForFunctionAtPositionTwoSnafu,
};
use crate::session_params::SessionParams;
use chrono_tz::Tz;
use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_expr::{
    ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[allow(clippy::unwrap_used)]
static RE_TIMEZONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(Z|[+-]\d{2}:?\d{2}|\b(?:CEST|CET|EEST|EET|EST|EDT|PST|PDT|MST|MDT|CST|CDT|GMT|UTC|MSD|JST)\b)$"
    ).unwrap()
});

const DATE_FORMATS: [&str; 2] = ["%d-%b-%Y", "%d-%B-%Y"];

const TIMESTAMP_FORMATS: [&str; 8] = [
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%YT%H:%M:%S",
    "%d-%b-%Y %H:%M:%S.%f",
    "%d-%b-%YT%H:%M:%S.%f",
    "%d-%B-%Y %H:%M:%S",
    "%d-%B-%YT%H:%M:%S",
    "%d-%B-%Y %H:%M:%S.%f",
    "%d-%B-%YT%H:%M:%S.%f",
];

#[derive(Debug)]
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
            "to_timestamp" => {
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
    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        if args.scalar_arguments.len() == 1 {
            if args.arg_types[0].is_numeric() {
                return Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    TimeUnit::Second,
                    self.timezone(),
                )));
            }
        } else if args.scalar_arguments.len() == 2 {
            if args.arg_types[0].is_numeric() {
                if let Some(v) = args.scalar_arguments[1] {
                    let scale = v.cast_to(&DataType::Int64)?;
                    let ScalarValue::Int64(Some(s)) = &scale else {
                        return ArgumentTwoNeedsToBeIntegerSnafu.fail()?;
                    };
                    let s = *s;
                    return match s {
                        0 => Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Second,
                            self.timezone(),
                        ))),
                        3 => Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Millisecond,
                            self.timezone(),
                        ))),
                        6 => Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Microsecond,
                            self.timezone(),
                        ))),
                        9 => Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Nanosecond,
                            self.timezone(),
                        ))),
                        _ => return InvalidValueForFunctionAtPositionTwoSnafu.fail()?,
                    };
                }
            } else if let Some(ScalarValue::TimestampSecond(_, Some(tz))) = args.scalar_arguments[0]
            {
                return Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    TimeUnit::Second,
                    Some(tz.to_owned()),
                )));
            } else if let Some(ScalarValue::TimestampMillisecond(_, Some(tz))) =
                args.scalar_arguments[0]
            {
                return Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some(tz.to_owned()),
                )));
            } else if let Some(ScalarValue::TimestampMicrosecond(_, Some(tz))) =
                args.scalar_arguments[0]
            {
                return Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(tz.to_owned()),
                )));
            } else if let Some(ScalarValue::TimestampNanosecond(_, Some(tz))) =
                args.scalar_arguments[0]
            {
                return Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(tz.to_owned()),
                )));
            }
        }

        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
            TimeUnit::Nanosecond,
            self.timezone(),
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
            Ok(if arr.len() == 1 {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
            } else {
                ColumnarValue::Array(Arc::new(arr))
            })
        } else if arr.data_type().is_integer() {
            let arr =
                kernels::cast::cast_with_options(&arr, &DataType::Int64, &DEFAULT_CAST_OPTIONS)?;

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

            Ok(if arr.len() == 1 {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
            } else {
                ColumnarValue::Array(Arc::new(arr))
            })
        } else if matches!(arr.data_type(), DataType::Timestamp(_, _)) {
            let DataType::Timestamp(_, tz) = arr.data_type() else {
                InvalidDataTypeSnafu.fail()?
            };

            let tz = if let Some(tz) = tz {
                Some(tz.clone())
            } else {
                self.timezone()
            };

            let arr = kernels::cast::cast_with_options(
                &arr,
                &DataType::Timestamp(TimeUnit::Nanosecond, tz),
                &DEFAULT_CAST_OPTIONS,
            )?;

            Ok(if arr.len() == 1 {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
            } else {
                ColumnarValue::Array(Arc::new(arr))
            })
        } else if matches!(arr.data_type(), DataType::Date32)
            || matches!(arr.data_type(), DataType::Date64)
        {
            let arr = kernels::cast::cast_with_options(
                &arr,
                &DataType::Timestamp(TimeUnit::Nanosecond, self.timezone()),
                &DEFAULT_CAST_OPTIONS,
            )?;

            Ok(if arr.len() == 1 {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
            } else {
                ColumnarValue::Array(Arc::new(arr))
            })
        } else if matches!(arr.data_type(), DataType::Decimal128(_, _)) {
            let DataType::Decimal128(_, s) = arr.data_type() else {
                InvalidDataTypeSnafu.fail()?
            };

            parse_decimal(&arr, &args, self.timezone(), *s)
        } else if matches!(arr.data_type(), DataType::Utf8)
            || matches!(arr.data_type(), DataType::Utf8View)
            || matches!(arr.data_type(), DataType::LargeUtf8)
        {
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

            let arr =
                kernels::cast::cast_with_options(&arr, &DataType::Utf8, &DEFAULT_CAST_OPTIONS)?;

            let arr: &StringArray = as_generic_string_array(&arr)?;

            let mut b = TimestampNanosecondBuilder::with_capacity(arr.len())
                .with_timezone_opt(self.timezone());
            for v in arr {
                match v {
                    None => b.append_null(),
                    Some(s) => {
                        if contains_only_digits(s) {
                            let i = s
                                .parse::<i64>()
                                .map_err(|_| CantParseTimestampSnafu.build())?;
                            let scale = determine_timestamp_scale(i);
                            if scale == 0 {
                                b.append_value(i * 1_000_000_000);
                            } else if scale == 3 {
                                b.append_value(i * 1_000_000);
                            } else if scale == 6 {
                                b.append_value(i * 1000);
                            } else if scale == 9 {
                                b.append_value(i);
                            }
                        } else {
                            let s = remove_timezone(s);
                            let t = if &format.to_ascii_lowercase() == "auto" {
                                let mut res: i64 = 0;
                                let mut found = false;
                                if let Ok(v) = string_to_timestamp_nanos(&s) {
                                    v
                                } else {
                                    for f in TIMESTAMP_FORMATS {
                                        if let Ok(v) = NaiveDateTime::parse_from_str(&s, f) {
                                            if let Some(vv) = v.and_utc().timestamp_nanos_opt() {
                                                res = vv;
                                                found = true;
                                                break;
                                            }

                                            if self.try_mode {
                                                b.append_null();
                                                continue;
                                            }

                                            return CantGetTimestampSnafu.fail()?;
                                        }
                                    }

                                    if !found {
                                        for f in DATE_FORMATS {
                                            if let Ok(v) = NaiveDate::parse_from_str(&s, f) {
                                                if let Some(vv) = v
                                                    .and_hms_nano_opt(0, 0, 0, 0)
                                                    .unwrap()
                                                    .and_utc()
                                                    .timestamp_nanos_opt()
                                                {
                                                    res = vv;
                                                    found = true;
                                                    break;
                                                }

                                                if self.try_mode {
                                                    b.append_null();
                                                    continue;
                                                }

                                                return CantGetTimestampSnafu.fail()?;
                                            }
                                        }

                                        if !found {
                                            if self.try_mode {
                                                b.append_null();
                                                continue;
                                            }

                                            return CantGetTimestampSnafu.fail()?;
                                        }
                                    }

                                    res
                                }
                            } else if let Ok(v) = NaiveDateTime::parse_from_str(&s, &format) {
                                if let Some(v) = v.and_utc().timestamp_nanos_opt() {
                                    v
                                } else {
                                    if self.try_mode {
                                        b.append_null();
                                        continue;
                                    }

                                    return CantGetTimestampSnafu.fail()?;
                                }
                            } else {
                                if self.try_mode {
                                    b.append_null();
                                    continue;
                                }

                                return CantGetTimestampSnafu.fail()?;
                            };

                            let t = if let Some(tz) = &self.timezone() {
                                let tz: Tz =
                                    tz.parse().map_err(|_| CantParseTimezoneSnafu.build())?;
                                let t = DateTime::from_timestamp_nanos(t);
                                let Some(t) = t.naive_utc().and_local_timezone(tz).single() else {
                                    if self.try_mode {
                                        b.append_null();
                                        continue;
                                    }

                                    return CantAddLocalTimezoneSnafu.fail()?;
                                };

                                let Some(t) = t.naive_utc().and_utc().timestamp_nanos_opt() else {
                                    if self.try_mode {
                                        b.append_null();
                                        continue;
                                    }

                                    return CantGetTimestampSnafu.fail()?;
                                };

                                t
                            } else {
                                t
                            };
                            b.append_value(t);
                        }
                    }
                }
            }

            let arr = Arc::new(b.finish()) as ArrayRef;
            Ok(if arr.len() == 1 {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
            } else {
                ColumnarValue::Array(Arc::new(arr))
            })
        } else {
            InvalidDataTypeSnafu.fail()?
        }
    }
}

#[allow(
    clippy::cast_possible_wrap,
    clippy::as_conversions,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::needless_pass_by_value
)]
fn parse_decimal(
    arr: &ArrayRef,
    args: &[ColumnarValue],
    timezone: Option<Arc<str>>,
    s: i8,
) -> DFResult<ColumnarValue> {
    let s = i128::from(s).pow(10);
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

    let s = datetime_str.trim();

    if let Some(caps) = RE_TIMEZONE.find(s) {
        return s[0..caps.start()].to_string();
    }

    s.to_string()
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

        let sql = "SELECT to_timestamp('2021-03-02 15:55:18.539000')";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------------------------------------------------+",
                "| to_timestamp(Utf8(\"2021-03-02 15:55:18.539000\")) |",
                "+--------------------------------------------------+",
                "| 2021-03-02T15:55:18.539                          |",
                "+--------------------------------------------------+",
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
       TO_TIMESTAMP('04/05/2024 01:02:03') as "b"
       "#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+---------------------+",
                "| a                   | b                   |",
                "+---------------------+---------------------+",
                "| 2024-04-05T01:02:03 | 2024-04-05T01:02:03 |",
                "+---------------------+---------------------+",
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

        let sql = "SELECT TO_TIMESTAMP('05-Mar-2025')";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------------------------+",
                "| to_timestamp(Utf8(\"05-Mar-2025\")) |",
                "+-----------------------------------+",
                "| 2025-03-05T00:00:00               |",
                "+-----------------------------------+",
            ],
            &result
        );

        let sql = "SELECT TO_TIMESTAMP('05-Mar-2025T01:02:03')";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------------------------------------+",
                "| to_timestamp(Utf8(\"05-Mar-2025T01:02:03\")) |",
                "+--------------------------------------------+",
                "| 2025-03-05T01:02:03                        |",
                "+--------------------------------------------+",
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
