use std::any::Any;
use std::sync::Arc;

use chrono::prelude::*;
use chrono::{Duration, Months};
use datafusion::arrow::array::builder::PrimitiveBuilder;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, AsArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::DataType::{Date32, Int32, Int64, Time64};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, Time64NanosecondType, TimestampNanosecondType,
};
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion_common::types::{logical_date, logical_int64, logical_string};
use datafusion_common::{_exec_datafusion_err, Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::arrow::datatypes::{Field, FieldRef};
use datafusion_macros::user_doc;

pub const UNIX_DAYS_FROM_CE: i32 = 719_163;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Creates a timestamp from individual numeric components.",
    syntax_example = "timestamp_from_parts(<year>, <month>, <day>, <hour>, <minute>, <second> [, <nanosecond> ] [, <time_zone> ] )",
    sql_example = "```sql
            > select timestamp_from_parts(2025, 2, 24, 12, 0, 50);
            +-----------------------------------------------------------------------------------+
            | timestamp_from_parts(Int64(2025),Int64(2),Int64(24),Int64(12),Int64(0),Int64(50)) |
            +-----------------------------------------------------------------------------------+
            | 1740398450.0                                                                      |
            +-----------------------------------------------------------------------------------+
            SELECT timestamp_from_parts(2025, 2, 24, 12, 0, 50, 65555555);
            +---------------------------------------------------------------------------------------------------+
            | timestamp_from_parts(Int64(2025),Int64(2),Int64(24),Int64(12),Int64(0),Int64(50),Int64(65555555)) |
            +---------------------------------------------------------------------------------------------------+
            | 1740398450.65555555                                                                               |
            +---------------------------------------------------------------------------------------------------+
```",
    argument(
        name = "year",
        description = "An integer expression to use as a year for building a timestamp."
    ),
    argument(
        name = "month",
        description = "An integer expression to use as a month for building a timestamp, with January represented as 1, and December as 12."
    ),
    argument(
        name = "day",
        description = "An integer expression to use as a day for building a timestamp, usually in the 1-31 range."
    ),
    argument(
        name = "hour",
        description = "An integer expression to use as an hour for building a timestamp, usually in the 0-23 range."
    ),
    argument(
        name = "minute",
        description = "An integer expression to use as a minute for building a timestamp, usually in the 0-59 range."
    ),
    argument(
        name = "second",
        description = "An integer expression to use as a second for building a timestamp, usually in the 0-59 range."
    ),
    argument(
        name = "date_expr",
        description = "Specifies the date expression to use for building a timestamp
         where date_expr provides the year, month, and day for the timestamp."
    ),
    argument(
        name = "time_expr",
        description = "Specifies the time expression to use for building a timestamp
         where time_expr provides the hour, minute, second, and nanoseconds within the day."
    ),
    argument(
        name = "nanoseconds",
        description = "Optional integer expression to use as a nanosecond for building a timestamp,
         usually in the 0-999999999 range."
    ),
    argument(
        name = "time_zone",
        description = "A string expression to use as a time zone for building a timestamp (e.g. America/Los_Angeles)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimestampFromPartsFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for TimestampFromPartsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TimestampFromPartsFunc {
    #[must_use]
    pub fn new() -> Self {
        let basic_signature =
            vec![Coercion::new_exact(TypeSignatureClass::Native(logical_int64())); 6];
        Self {
            signature: Signature::one_of(
                vec![
                    // TIMESTAMP_FROM_PARTS( <year>, <month>, <day>, <hour>, <minute>, <second> [, <nanosecond> ] [, <time_zone> ] )
                    Coercible(basic_signature.clone()),
                    Coercible(
                        [
                            basic_signature.clone(),
                            vec![Coercion::new_exact(TypeSignatureClass::Native(
                                logical_int64(),
                            ))],
                        ]
                        .concat(),
                    ),
                    Coercible(
                        [
                            basic_signature,
                            vec![
                                Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                                Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                            ],
                        ]
                        .concat(),
                    ),
                    // TIMESTAMP_FROM_PARTS( <date_expr>, <time_expr> )
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("timestamp_ntz_from_parts"),
                String::from("timestamp_tz_from_parts"),
                String::from("timestamp_ltz_from_parts"),
                String::from("timestampntzfromparts"),
                String::from("timestamptzfromparts"),
                String::from("timestampltzfromparts"),
            ],
        }
    }
}
impl ScalarUDFImpl for TimestampFromPartsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "timestamp_from_parts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args should be called")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() == 8
            && let Some(ScalarValue::Utf8(Some(tz))) = args.scalar_arguments[7]
        {
            return Ok(Arc::new(Field::new(
                self.name(),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(tz.clone()))),
                true,
            )));
        }
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )))
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let array_size = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let is_scalar = array_size == 1;

        // TIMESTAMP_FROM_PARTS( <date_expr>, <time_expr> )
        let result = if args.len() == 2 {
            let [date, time] = take_function_args(self.name(), args)?;
            let date = to_primitive_array::<Date32Type>(&date.cast_to(&Date32, None)?)?;
            let time = to_primitive_array::<Time64NanosecondType>(
                &time.cast_to(&Time64(TimeUnit::Nanosecond), None)?,
            )?;
            let mut builder = PrimitiveArray::builder(array_size);
            for i in 0..array_size {
                let ts = make_timestamp_from_date_time(date.value(i), time.value(i))?;
                builder.append_value(ts);
            }
            Ok(builder.finish())
        } else if args.len() > 5 && args.len() < 9 {
            // TIMESTAMP_FROM_PARTS( <year>, <month>, <day>, <hour>, <minute>, <second> [, <nanosecond> ] [, <time_zone> ] )
            timestamps_from_components(args, array_size)
        } else {
            internal_err!("Unsupported number of arguments")
        }?;

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(result))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn timestamps_from_components(
    args: &[ColumnarValue],
    array_size: usize,
) -> Result<PrimitiveArray<TimestampNanosecondType>> {
    let (years, months, days, hours, minutes, seconds, nanoseconds, time_zone) = match args.len() {
        8 => {
            let [
                years,
                months,
                days,
                hours,
                minutes,
                seconds,
                nanoseconds,
                time_zone,
            ] = take_function_args("timestamp_from_parts", args)?;
            (
                years,
                months,
                days,
                hours,
                minutes,
                seconds,
                Some(nanoseconds),
                Some(time_zone),
            )
        }
        7 => {
            let [years, months, days, hours, minutes, seconds, nanoseconds] =
                take_function_args("timestamp_from_parts", args)?;
            (
                years,
                months,
                days,
                hours,
                minutes,
                seconds,
                Some(nanoseconds),
                None,
            )
        }
        6 => {
            let [years, months, days, hours, minutes, seconds] =
                take_function_args("timestamp_from_parts", args)?;
            (years, months, days, hours, minutes, seconds, None, None)
        }
        _ => return internal_err!("Unsupported number of arguments"),
    };
    let years = to_primitive_array::<Int32Type>(&years.cast_to(&Int32, None)?)?;
    let months = to_primitive_array::<Int32Type>(&months.cast_to(&Int32, None)?)?;
    let days = to_primitive_array::<Int32Type>(&days.cast_to(&Int32, None)?)?;
    let hours = to_primitive_array::<Int64Type>(&hours.cast_to(&Int64, None)?)?;
    let minutes = to_primitive_array::<Int64Type>(&minutes.cast_to(&Int64, None)?)?;
    let seconds = to_primitive_array::<Int64Type>(&seconds.cast_to(&Int64, None)?)?;
    let nanoseconds = nanoseconds
        .map(|nanoseconds| to_primitive_array::<Int64Type>(&nanoseconds.cast_to(&Int64, None)?))
        .transpose()?;
    let time_zone = time_zone.map(to_string_array).transpose()?;

    let mut builder: PrimitiveBuilder<TimestampNanosecondType> =
        PrimitiveArray::builder(array_size);
    for i in 0..array_size {
        let nanoseconds = nanoseconds.as_ref().map(|ns| ns.value(i));
        let time_zone = time_zone.as_ref().map(|tz| tz.value(i));
        let ts = make_timestamp(
            years.value(i),
            months.value(i),
            days.value(i),
            hours.value(i),
            minutes.value(i),
            seconds.value(i),
            nanoseconds,
            time_zone,
        )?;
        builder.append_value(ts);
    }
    Ok(builder.finish())
}

fn make_timestamp_from_date_time(date: i32, time: i64) -> Result<i64> {
    make_timestamp_from_nanoseconds(i64::from(date) * 86_400_000_000_000 + time, None)
}

pub fn make_date(year: i32, month: i32, days: i32) -> Result<i32> {
    let u_month = match month {
        0 => 1,
        _ if month < 0 => 1 - month,
        _ => month - 1,
    };
    let u_month = u32::try_from(u_month)
        .map_err(|_| _exec_datafusion_err!("month value '{month:?}' is out of range"))?;

    NaiveDate::from_ymd_opt(year, 1, 1).map_or_else(
        || exec_err!("Invalid date part '{year:?}' '{month:?}' '{days:?}'"),
        |date| {
            let months = Months::new(u_month);
            let days = Duration::days(i64::from(days - 1));
            let result = if month <= 0 {
                date.checked_sub_months(months)
            } else {
                date.checked_add_months(months)
            };
            result.map_or_else(
                || exec_err!("invalid date part '{year:?}' '{month:?}' '{days:?}'"),
                |months_result| {
                    months_result.checked_add_signed(days).map_or_else(
                        || exec_err!("invalid date part '{year:?}' '{month:?}' '{days:?}'"),
                        |days_result| Ok(days_result.num_days_from_ce() - UNIX_DAYS_FROM_CE),
                    )
                },
            )
        },
    )
}

#[must_use]
pub fn make_time(hour: i64, minute: i64, seconds: i64, nanosecond: Option<i64>) -> i64 {
    let n_hour = hour * 3_600_000_000_000;
    let n_minute = minute * 60_000_000_000;
    let n_seconds = seconds * 1_000_000_000;
    let n_nano = nanosecond.unwrap_or(0);
    n_hour + n_minute + n_seconds + n_nano
}

#[allow(clippy::too_many_arguments)]
fn make_timestamp(
    year: i32,
    month: i32,
    day: i32,
    hour: i64,
    minute: i64,
    seconds: i64,
    nano: Option<i64>,
    timezone: Option<&str>,
) -> Result<i64> {
    let days = make_date(year, month, day)?;
    let n_date = i64::from(days) * 86_400_000_000_000;
    let n_time = make_time(hour, minute, seconds, nano);
    let total_nanos = n_date + n_time;
    make_timestamp_from_nanoseconds(total_nanos, timezone)
}

fn make_timestamp_from_nanoseconds(nanoseconds: i64, tz: Option<&str>) -> Result<i64> {
    let date_time = DateTime::from_timestamp_nanos(nanoseconds);
    let timestamp = if let Some(timezone) = tz {
        date_time
            .with_timezone(&timezone.parse::<Tz>()?)
            .timestamp_nanos_opt()
    } else {
        date_time.timestamp_nanos_opt()
    };
    timestamp.map_or_else(
        || exec_err!("Unable to parse timestamp from '{:?}'", nanoseconds),
        Ok,
    )
}

pub fn take_function_args<const N: usize, T>(
    function_name: &str,
    args: impl IntoIterator<Item = T>,
) -> Result<[T; N]> {
    let args = args.into_iter().collect::<Vec<_>>();
    args.try_into().map_err(|v: Vec<T>| {
        _exec_datafusion_err!(
            "{} function requires {} {}, got {}",
            function_name,
            N,
            if N == 1 { "argument" } else { "arguments" },
            v.len()
        )
    })
}

pub fn to_primitive_array<T>(col: &ColumnarValue) -> Result<PrimitiveArray<T>>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType,
{
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<T>().to_owned()),
        ColumnarValue::Scalar(scalar) => {
            let value = scalar.to_array()?;
            Ok(value.as_primitive::<T>().to_owned())
        }
    }
}

fn to_string_array(col: &ColumnarValue) -> Result<StringArray> {
    match col {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(std::borrow::ToOwned::to_owned)
            .ok_or_else(|| _exec_datafusion_err!("Failed to downcast Array to StringArray")),
        ColumnarValue::Scalar(scalar) => {
            let value = scalar.to_array()?;
            value
                .as_any()
                .downcast_ref::<StringArray>()
                .map(std::borrow::ToOwned::to_owned)
                .ok_or_else(|| _exec_datafusion_err!("Failed to downcast Scalar to StringArray"))
        }
    }
}

crate::macros::make_udf_function!(TimestampFromPartsFunc);

#[cfg(test)]
mod test {
    use super::{TimestampFromPartsFunc, to_primitive_array};
    use chrono::DateTime;
    use datafusion::arrow::datatypes::TimestampNanosecondType;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;
    use datafusion_expr::ScalarUDFImpl;

    #[allow(clippy::unwrap_used)]
    fn columnar_value_fn<T>(is_scalar: bool, v: T) -> ColumnarValue
    where
        ScalarValue: From<T>,
        T: Clone,
    {
        if is_scalar {
            ColumnarValue::Scalar(ScalarValue::from(v))
        } else {
            ColumnarValue::Array(ScalarValue::from(v).to_array().unwrap())
        }
    }

    #[allow(clippy::type_complexity, clippy::too_many_lines, clippy::unwrap_used)]
    #[test]
    fn test_timestamp_from_parts_components() {
        let args: [(
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<i64>,
            Option<String>,
            String,
        ); 7] = [
            (
                2025,
                1,
                2,
                0,
                0,
                0,
                None,
                None,
                "2025-01-02 00:00:00.000000000".to_string(),
            ),
            (
                2025,
                1,
                2,
                12,
                0,
                0,
                Some(0),
                None,
                "2025-01-02 12:00:00.000000000".to_string(),
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                0,
                Some(0),
                None,
                "2025-01-02 12:10:00.000000000".to_string(),
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(0),
                None,
                "2025-01-02 12:10:12.000000000".to_string(),
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(500),
                None,
                "2025-01-02 12:10:12.000000500".to_string(),
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(0),
                Some("America/Los_Angeles".to_string()),
                "2025-01-02 12:10:12.000000000".to_string(),
            ),
            (
                2025,
                -5,
                -15,
                -12,
                0,
                -3600,
                None,
                None,
                "2024-06-14 11:00:00.000000000".to_string(),
            ),
        ];

        let is_scalar_type = [true, false];

        for is_scalar in is_scalar_type {
            for (i, (y, m, d, h, mi, s, n, tz, exp)) in args.iter().enumerate() {
                let mut fn_args = vec![
                    columnar_value_fn(is_scalar, *y),
                    columnar_value_fn(is_scalar, *m),
                    columnar_value_fn(is_scalar, *d),
                    columnar_value_fn(is_scalar, *h),
                    columnar_value_fn(is_scalar, *mi),
                    columnar_value_fn(is_scalar, *s),
                ];
                if let Some(nano) = n {
                    fn_args.push(columnar_value_fn(is_scalar, *nano));
                }
                if let Some(t) = tz {
                    fn_args.push(columnar_value_fn(is_scalar, t.to_string()));
                }
                let arg_fields = fn_args
                    .iter()
                    .map(|cv| match cv {
                        ColumnarValue::Array(a) => Arc::new(Field::new("arg", a.data_type().clone(), true)),
                        ColumnarValue::Scalar(s) => Arc::new(Field::new("arg", s.data_type(), true)),
                    })
                    .collect();
                let return_field = Arc::new(Field::new(
                    "result",
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ));
                let result = TimestampFromPartsFunc::new()
                    .invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                        args: fn_args,
                        arg_fields,
                        number_rows: 1,
                        return_field,
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap();
                let result = to_primitive_array::<TimestampNanosecondType>(&result).unwrap();
                let ts = DateTime::from_timestamp_nanos(result.value(0))
                    .format("%Y-%m-%d %H:%M:%S.%f")
                    .to_string();
                assert_eq!(ts, *exp, "failed at index {i}");
            }
        }
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_timestamp_from_parts_exp() {
        let args: [(i32, i64, i64); 2] = [
            (20143, 39_075_773_219_000, 1_740_394_275_773_219_000),
            (20143, 0, 1_740_355_200_000_000_000),
        ];

        let is_scalar_type = [true, false];

        for is_scalar in is_scalar_type {
            for (i, (date, time, exp)) in args.iter().enumerate() {
                let fn_args = vec![
                    columnar_value_fn(is_scalar, ScalarValue::Date32(Some(*date))),
                    columnar_value_fn(is_scalar, ScalarValue::Time64Nanosecond(Some(*time))),
                ];
                let arg_fields = fn_args
                    .iter()
                    .map(|cv| match cv {
                        ColumnarValue::Array(a) => Arc::new(Field::new("arg", a.data_type().clone(), true)),
                        ColumnarValue::Scalar(s) => Arc::new(Field::new("arg", s.data_type(), true)),
                    })
                    .collect();
                let return_field = Arc::new(Field::new(
                    "result",
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ));
                let result = TimestampFromPartsFunc::new()
                    .invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                        args: fn_args,
                        arg_fields,
                        number_rows: 1,
                        return_field,
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap();
                let result = to_primitive_array::<TimestampNanosecondType>(&result).unwrap();
                assert_eq!(result.value(0), *exp, "failed at index {i}");
            }
        }
    }
}
