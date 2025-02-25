// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::sync::Arc;

use arrow::array::builder::PrimitiveBuilder;
use arrow::array::timezone::Tz;
use arrow::array::{Array, AsArray, PrimitiveArray, StringArray};
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, Time64NanosecondType, TimestampNanosecondType,
};
use arrow_schema::DataType::{Date32, Int32, Int64, Time64};
use arrow_schema::TimeUnit;
use chrono::prelude::*;
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion_common::types::{logical_int64, logical_string};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue, _exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

const UNIX_EPOCH_DAYS: i32 = 719_163;

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
    standard_argument(name = "str", prefix = "String"),
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
#[derive(Debug)]
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
    pub fn new() -> Self {
        let basic_signature = vec![TypeSignatureClass::Native(logical_int64()); 6];
        Self {
            signature: Signature::one_of(
                vec![
                    // TIMESTAMP_FROM_PARTS( <year>, <month>, <day>, <hour>, <minute>, <second> [, <nanosecond> ] [, <time_zone> ] )
                    Coercible(basic_signature.clone()),
                    Coercible(
                        [
                            basic_signature.clone(),
                            vec![TypeSignatureClass::Native(logical_int64())],
                        ]
                        .concat(),
                    ),
                    Coercible(
                        [
                            basic_signature,
                            vec![
                                TypeSignatureClass::Native(logical_int64()),
                                TypeSignatureClass::Native(logical_string()),
                            ],
                        ]
                        .concat(),
                    ),
                    // TIMESTAMP_FROM_PARTS( <date_expr>, <time_expr> )
                    Coercible(vec![TypeSignatureClass::Date, TypeSignatureClass::Time]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("timestamp_ntz_from_parts"),
                String::from("timestamp_tz_from_parts"),
                String::from("timestamp_ltz_from_parts"),
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

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        if args.arg_types.len() == 8 {
            if let Some(ScalarValue::Utf8(Some(tz))) = args.scalar_arguments[7] {
                return Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from(tz.clone())),
                )));
            }
        }
        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
            TimeUnit::Nanosecond,
            None,
        )))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
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
                make_timestamp_from_date_time(date.value(i), time.value(i), &mut builder)?;
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
            let [years, months, days, hours, minutes, seconds, nanoseconds, time_zone] =
                take_function_args("timestamp_from_parts", args)?;
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
    let hours = to_primitive_array::<Int32Type>(&hours.cast_to(&Int32, None)?)?;
    let minutes = to_primitive_array::<Int32Type>(&minutes.cast_to(&Int32, None)?)?;
    let seconds = to_primitive_array::<Int32Type>(&seconds.cast_to(&Int32, None)?)?;
    let nanoseconds = nanoseconds
        .map(|nanoseconds| to_primitive_array::<Int64Type>(&nanoseconds.cast_to(&Int64, None)?))
        .transpose()?;
    let time_zone = time_zone.map(to_string_array).transpose()?;

    let mut builder: PrimitiveBuilder<TimestampNanosecondType> =
        PrimitiveArray::builder(array_size);
    for i in 0..array_size {
        let nanoseconds = nanoseconds.as_ref().map(|ns| ns.value(i));
        let time_zone = time_zone.as_ref().map(|tz| tz.value(i));
        make_timestamp(
            years.value(i),
            months.value(i),
            days.value(i),
            hours.value(i),
            minutes.value(i),
            seconds.value(i),
            nanoseconds,
            time_zone,
            &mut builder,
        )?;
    }
    Ok(builder.finish())
}

fn make_timestamp_from_date_time(
    days: i32,
    time_nano: i64,
    builder: &mut PrimitiveBuilder<TimestampNanosecondType>,
) -> Result<()> {
    let seconds = u32::try_from(time_nano / 1_000_000_000)
        .map_err(|_| _exec_datafusion_err!("time seconds value '{time_nano:?}' is out of range"))?;
    let nanoseconds = u32::try_from(time_nano % 1_000_000_000).map_err(|_| {
        _exec_datafusion_err!("time nanoseconds value '{time_nano:?}' is out of range")
    })?;

    if let Some(naive_date) = NaiveDate::from_num_days_from_ce_opt(days + UNIX_EPOCH_DAYS) {
        if let Some(naive_time) =
            NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds)
        {
            if let Some(timestamp) = naive_date
                .and_time(naive_time)
                .and_utc()
                .timestamp_nanos_opt()
            {
                builder.append_value(timestamp);
            } else {
                return exec_err!(
                    "Unable to parse timestamp from date '{:?}' and time '{:?}'",
                    days,
                    time_nano
                );
            }
        } else {
            return exec_err!("Invalid time part '{:?}'", time_nano);
        }
    } else {
        return exec_err!("Invalid date part '{:?}'", days);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn make_timestamp(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    seconds: i32,
    nanosecond: Option<i64>,
    time_zone: Option<&str>,
    builder: &mut PrimitiveBuilder<TimestampNanosecondType>,
) -> Result<()> {
    let day = u32::try_from(day)
        .map_err(|_| _exec_datafusion_err!("day value '{day:?}' is out of range"))?;
    let month = u32::try_from(month)
        .map_err(|_| _exec_datafusion_err!("month value '{month:?}' is out of range"))?;
    let hour = u32::try_from(hour)
        .map_err(|_| _exec_datafusion_err!("hour value '{hour:?}' is out of range"))?;
    let minute = u32::try_from(minute)
        .map_err(|_| _exec_datafusion_err!("minute value '{minute:?}' is out of range"))?;
    let seconds = u32::try_from(seconds)
        .map_err(|_| _exec_datafusion_err!("seconds value '{seconds:?}' is out of range"))?;
    let nano = u32::try_from(nanosecond.unwrap_or(0))
        .map_err(|_| _exec_datafusion_err!("nanosecond value '{nanosecond:?}' is out of range"))?;

    if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
        if let Some(time) = NaiveTime::from_hms_nano_opt(hour, minute, seconds, nano) {
            let date_time = date.and_time(time);
            let timestamp = if let Some(time_zone) = time_zone {
                Utc.from_utc_datetime(&date_time)
                    .with_timezone(&time_zone.parse::<Tz>()?)
                    .timestamp_nanos_opt()
            } else {
                date_time.and_utc().timestamp_nanos_opt()
            };

            if let Some(ts) = timestamp {
                builder.append_value(ts);
            } else {
                return exec_err!(
                    "Unable to parse timestamp from date '{date:?}' and time '{time:?}'"
                );
            }
        } else {
            return exec_err!("Invalid time part '{hour:?}':'{minute:?}':'{seconds:?}'");
        }
    } else {
        return exec_err!("Invalid date part '{year:?}'-'{month:?}'-'{day:?}'");
    }
    Ok(())
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

fn to_primitive_array<T>(col: &ColumnarValue) -> Result<PrimitiveArray<T>>
where
    T: arrow::datatypes::ArrowPrimitiveType,
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

super::macros::make_udf_function!(TimestampFromPartsFunc);

#[cfg(test)]
mod test {
    use crate::datafusion::functions::timestamp_from_parts::{
        to_primitive_array, TimestampFromPartsFunc,
    };
    use arrow::datatypes::TimestampNanosecondType;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::ScalarValue;
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

    #[allow(clippy::type_complexity, clippy::unwrap_used, clippy::too_many_lines)]
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
            i64,
        ); 7] = [
            (2025, 1, 2, 0, 0, 0, None, None, 1_735_776_000_000_000_000),
            (
                2025,
                1,
                2,
                12,
                0,
                0,
                Some(0),
                Some("UTC".to_string()),
                1_735_819_200_000_000_000,
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                0,
                Some(0),
                Some("America/New_York".to_string()),
                1_735_819_800_000_000_000,
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(0),
                Some("Asia/Tokyo".to_string()),
                1_735_819_812_000_000_000,
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(0),
                Some("Europe/London".to_string()),
                1_735_819_812_000_000_000,
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(0),
                Some("Africa/Cairo".to_string()),
                1_735_819_812_000_000_000,
            ),
            (
                2025,
                1,
                2,
                12,
                10,
                12,
                Some(10),
                Some("Australia/Sydney".to_string()),
                1_735_819_812_000_000_010,
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
                };
                if let Some(t) = tz {
                    fn_args.push(columnar_value_fn(is_scalar, t.to_string()));
                };
                let result = TimestampFromPartsFunc::new()
                    .invoke_batch(&fn_args, 1)
                    .unwrap();
                let result = to_primitive_array::<TimestampNanosecondType>(&result).unwrap();
                assert_eq!(result.value(0), *exp, "failed at index {i}");
            }
        }
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_timestamp_from_parts_exp() {
        // TypeSignatureClass::Date, TypeSignatureClass::Time
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
                let result = TimestampFromPartsFunc::new()
                    .invoke_batch(&fn_args, 1)
                    .unwrap();
                let result = to_primitive_array::<TimestampNanosecondType>(&result).unwrap();
                assert_eq!(result.value(0), *exp, "failed at index {i}");
            }
        }
    }
}
