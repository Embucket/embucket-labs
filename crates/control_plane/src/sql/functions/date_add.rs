use arrow::array::Array;
use arrow::datatypes::DataType::{
    Date32, Date64, Int64, Time32, Time64, Timestamp, Utf8,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{ArrowNativeType, DataType, Fields};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};
use datafusion::scalar::ScalarValue;
use std::any::Any;
use arrow::array::types::{IntervalMonthDayNano, IntervalDayTime};

#[derive(Debug)]
pub struct DateAddFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for DateAddFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateAddFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Int64, Date32]),
                    Exact(vec![Utf8, Int64, Date64]),

                    Exact(vec![Utf8, Int64, Time32(Second)]),
                    Exact(vec![Utf8, Int64, Time32(Nanosecond)]),
                    Exact(vec![Utf8, Int64, Time32(Microsecond)]),
                    Exact(vec![Utf8, Int64, Time32(Millisecond)]),

                    Exact(vec![Utf8, Int64, Time64(Second)]),
                    Exact(vec![Utf8, Int64, Time64(Nanosecond)]),
                    Exact(vec![Utf8, Int64, Time64(Microsecond)]),
                    Exact(vec![Utf8, Int64, Time64(Millisecond)]),

                    Exact(vec![Utf8, Int64, Timestamp(Second, None)]),
                    Exact(vec![Utf8, Int64, Timestamp(Millisecond, None)]),
                    Exact(vec![Utf8, Int64, Timestamp(Microsecond, None)]),
                    Exact(vec![Utf8, Int64, Timestamp(Nanosecond, None)]),

                    Exact(vec![
                        Utf8,
                        Int64,
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Int64,
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Int64,
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Int64,
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("date_add"),
                String::from("time_add"),
                String::from("timeadd"),
                String::from("timestamp_add"),
                String::from("timestampadd"),
            ],
        }
    }
}

/// dateadd SQL function
/// Syntax: `DATEADD(<date_or_time_part>, <value>, <date_or_time_expr>)`
/// - <date_or_time_part>: This indicates the units of time that you want to add.
/// For example if you want to add two days, then specify day. This unit of measure must be one of the values listed in Supported date and time parts.
/// - <value>: This is the number of units of time that you want to add.
/// For example, if the units of time is day, and you want to add two days, specify 2. If you want to subtract two days, specify -2.
/// - <date_or_time_expr>: Must evaluate to a date, time, or timestamp.
/// This is the date, time, or timestamp to which you want to add.
/// For example, if you want to add two days to August 1, 2024, then specify '2024-08-01'::DATE.
/// If the data type is TIME, then the date_or_time_part must be in units of hours or smaller, not days or bigger.
/// If the input data type is DATE, and the date_or_time_part is hours or smaller, the input value will not be rejected,
/// but instead will be treated as a TIMESTAMP with hours, minutes, seconds, and fractions of a second all initially set to 0 (e.g. midnight on the specified date).
///
/// Note: `dateadd` returns
/// If date_or_time_expr is a time, then the return data type is a time.
/// If date_or_time_expr is a timestamp, then the return data type is a timestamp.
/// If date_or_time_expr is a date:
/// - If date_or_time_part is day or larger (for example, month, year), the function returns a DATE value.
/// - If date_or_time_part is smaller than a day (for example, hour, minute, second), the function returns a TIMESTAMP_NTZ value, with 00:00:00.000 as the starting time for the date.
/// Usage notes:
/// - When date_or_time_part is year, quarter, or month (or any of their variations),
/// if the result month has fewer days than the original day of the month, the result day of the month might be different from the original day.
/// Examples
/// - dateadd(day, 30, CAST('2024-12-26' AS TIMESTAMP))
impl ScalarUDFImpl for DateAddFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dateadd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("function requires three arguments");
        }
        println!("arg_types: {:?}", arg_types);
        Ok(arg_types[2].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return plan_err!("function requires three arguments");
        }
        println!("args: {:?}", args);

        let date_or_time_part = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
            _ => return plan_err!("Invalid unit type format"),
        };

        let value = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(val))) => *val,
            _ => return plan_err!("Invalid value type"),
        };
        // TODO add logic
        let date_or_time_expr = match &args[2] {
            ColumnarValue::Scalar(val) => val.clone(),
            _ => return plan_err!("Invalid datetime type"),
        };
        //there should be overflows TODO: CHECK and FIX
        match date_or_time_part.as_str() {
            //TODO: should consider leap year (365-366 days)
            "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" | "year" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 12 * 365 * 86_400_000_000_000))).unwrap())),
            //TODO: should consider months 28-31 days
            "mm" | "mon" | "mons" | "months" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 30 * 86_400_000_000_000))).unwrap())),
            "d" | "dd" | "days" | "dayofmonth" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 86_400_000_000_000))).unwrap())),
            "w" | "wk" | "weekofyear" | "woy" | "wy" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 7 * 86_400_000_000_000))).unwrap())),
            //TODO: should consider months 28-31 days
            "q" | "qtr" | "qtrs" | "quarters" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 3 * 30 * 86_400_000_000_000))).unwrap())),
            "h" | "hh" | "hr" | "hours" | "hrs" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 3_600_000_000_000))).unwrap())),
            "m" | "mi" | "min" | "minutes" | "mins" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 60_000_000_000))).unwrap())),
            "s" | "sec" | "seconds" | "secs" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 1_000_000_000))).unwrap())),
            "ms" | "msec" | "millieseconds" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 1_000_000))).unwrap())),
            "us" | "usec" | "microseconds" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value * 1000))).unwrap())),
            "ns" | "nsec" | "nanosec" | "nsecond" | "nanoseconds" | "nanosecs" | "nseconds" => Ok(ColumnarValue::Scalar(date_or_time_expr.add(ScalarValue::DurationNanosecond(Some(value))).unwrap())),
            _ => {
                return plan_err!("Invalid date_or_time_part type")
            },
        }
    }
}
