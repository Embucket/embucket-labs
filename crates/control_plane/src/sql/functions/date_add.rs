use arrow::array::Array;
use arrow::datatypes::DataType::{Date32, Date64, Int64, Time32, Time64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, Fields};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};
use datafusion::scalar::ScalarValue;
use std::any::Any;

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

    fn add_nanoseconds(val: &ScalarValue, nanoseconds: i64) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            val.add(ScalarValue::DurationNanosecond(Some(nanoseconds)))
                .unwrap_or(ScalarValue::DurationNanosecond(Some(0))),
        ))
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
        Ok(arg_types[2].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return plan_err!("function requires three arguments");
        }

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
        //TODO: there should be overflows CHECK and FIX
        match date_or_time_part.as_str() {
            //TODO: should consider leap year (365-366 days)
            "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" | "year" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 12 * 365 * 86_400_000_000_000),
            //TODO: should consider months 28-31 days
            "mm" | "mon" | "mons" | "months" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 30 * 86_400_000_000_000),
            "d" | "dd" | "days" | "dayofmonth" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 86_400_000_000_000),
            "w" | "wk" | "weekofyear" | "woy" | "wy" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 7 * 86_400_000_000_000),
            //TODO: should consider months 28-31 days
            "q" | "qtr" | "qtrs" | "quarters" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 3 * 30 * 86_400_000_000_000),
            "h" | "hh" | "hr" | "hours" | "hrs" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 3_600_000_000_000),
            "m" | "mi" | "min" | "minutes" | "mins" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 60_000_000_000),
            "s" | "sec" | "seconds" | "secs" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 1_000_000_000),
            "ms" | "msec" | "millieseconds" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 1_000_000),
            "us" | "usec" | "microseconds" => DateAddFunc::add_nanoseconds(
                &date_or_time_expr, value * 1000),
            "ns" | "nsec" | "nanosec" | "nsecond" | "nanoseconds" | "nanosecs" | "nseconds" =>
                DateAddFunc::add_nanoseconds(&date_or_time_expr, value),
            _ => return plan_err!("Invalid date_or_time_part type"),
        }
    }
}
