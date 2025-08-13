use super::errors as dtime_errors;
use crate::session_params::SessionParams;
use arrow_schema::TimeUnit;
use datafusion::arrow::array::{Array, ArrayRef, DurationNanosecondArray, Int32Array, Int64Array};
use datafusion::arrow::compute::kernels::numeric::sub;
use datafusion::arrow::compute::{DatePart, cast, date_part};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_common::cast::{as_int32_array, as_int64_array};
use datafusion_common::types::logical_date;
use datafusion_common::{internal_err, types::logical_string};
use datafusion_expr::Coercion;
use std::any::Any;
use std::sync::Arc;
use std::vec;

const SECOND: i64 = 1_000_000_000;

/// `DATEDIFF` SQL function
///
/// Calculates the difference between two date, time, or timestamp expressions based
/// on the date or time part requested. The function returns the result of subtracting
/// the second argument from the third argument.
/// Syntax: `DATEDIFF( <date_or_time_part>, <date_or_time_expr1>, <date_or_time_expr2> )`
///
/// Arguments:
/// - `date_or_time_part`: The unit of time
/// - `date_or_time_expr1` and `date_or_time_expr2`: The values to compare. Must be a date, a time,
///   a timestamp, or an expression that can be evaluated to a date, a time, or a timestamp.
///
/// Example: `DATEDIFF(month, '2021-01-01'::DATE, '2021-02-28'::DATE)`
///
/// Returns:
/// - This function returns a value of type DATE, even if `date_or_time_part` is a time.
#[derive(Debug)]
pub struct DateDiffFunc {
    signature: Signature,
    #[allow(dead_code)]
    aliases: Vec<String>,
    session_params: Arc<SessionParams>,
}

impl Default for DateDiffFunc {
    fn default() -> Self {
        Self::new(Arc::new(SessionParams::default()))
    }
}

impl DateDiffFunc {
    #[must_use]
    pub fn new(session_params: Arc<SessionParams>) -> Self {
        Self {
            //TODO: Fix signature, can we diffretite between two differnt types? (ex.: date32 - timestamp)
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("date_diff"),
                String::from("timediff"),
                String::from("time_diff"),
                String::from("timestampdiff"),
                String::from("timestamp_diff"),
            ],
            session_params,
        }
    }

    #[must_use]
    pub fn week_start(&self) -> i64 {
        self.session_params
            .get_property("week_start")
            .map_or_else(|| 0, |v| v.parse::<i64>().unwrap_or(0))
    }

    fn date_diff_func(
        &self,
        lhs: &Arc<dyn Array>,
        rhs: &Arc<dyn Array>,
        unit_type: DatePart,
    ) -> Result<ColumnarValue> {
        let arr1 = cast(lhs, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
        let arr2 = cast(rhs, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
        let diff = sub(&arr2, &arr1)?;
        let diff_arr = diff
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .ok_or_else(|| dtime_errors::CantCastToSnafu { v: "duration_nsec" }.build())?;
        match unit_type {
            DatePart::Quarter | DatePart::Year | DatePart::YearISO => {
                let arr1 = &date_part(&arr1, unit_type)?;
                let arr2 = &date_part(&arr2, unit_type)?;
                let diff = cast(&sub(&arr2, &arr1)?, &DataType::Int64)?;
                Ok(ColumnarValue::Array(Arc::new(diff)))
            }
            DatePart::Month => {
                let month1 = &date_part(&arr1, unit_type)?;
                let month2 = &date_part(&arr2, unit_type)?;
                let diff_month = sub(&month2, &month1)?;
                let month_arr = as_int32_array(&diff_month)?;

                let year1 = &date_part(&arr1, DatePart::Year)?;
                let year2 = &date_part(&arr2, DatePart::Year)?;
                let diff_year = sub(&year2, &year1)?;
                let year_arr = as_int32_array(&diff_year)?;

                let result = year_arr
                    .iter()
                    .zip(month_arr.iter())
                    .map(|(y, m)| match (y, m) {
                        (Some(y), Some(m)) => Some(y * 12 + m),
                        _ => None,
                    })
                    .collect::<Int32Array>();
                let result = cast(&result, &DataType::Int64)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DatePart::Week | DatePart::WeekISO => Ok(self.weeks_diff(diff_arr)),
            DatePart::Day | DatePart::DayOfYear => Ok(Self::diff(diff_arr, 86_400 * SECOND)),
            DatePart::Hour => {
                let nanos_in_hour: i64 = 3_600 * SECOND;
                let arr1 = &date_part(&arr1, unit_type)?;
                let arr2 = &date_part(&arr2, unit_type)?;
                let hours_diff = cast(&sub(&arr2, &arr1)?, &DataType::Int64)?;
                let hours_arr = as_int64_array(&hours_diff)?;

                let result = diff_arr
                    .iter()
                    .zip(hours_arr.iter())
                    .map(|(nanos, diff)| match (nanos, diff) {
                        (Some(n), Some(hours_diff)) => {
                            let res = n.div_euclid(nanos_in_hour);
                            if hours_diff != 0 {
                                Some(res + 1)
                            } else {
                                Some(res)
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DatePart::Minute => Ok(Self::diff(diff_arr, 60 * SECOND)),
            DatePart::Second => Ok(Self::diff(diff_arr, SECOND)),
            DatePart::Millisecond => Ok(Self::diff(diff_arr, 1_000_000)),
            DatePart::Microsecond => Ok(Self::diff(diff_arr, 1_000)),
            _ => Ok(Self::diff(diff_arr, 1)),
        }
    }

    fn weeks_diff(&self, diff_arr: &DurationNanosecondArray) -> ColumnarValue {
        let week_start = self.week_start();
        let nanos_in_day: i64 = 86_400_000_000_000;

        let diff: Int64Array = diff_arr.unary(|ns| {
            let days = ns.div_euclid(nanos_in_day);
            if days < 0 {
                0
            } else {
                let weeks = days.div_euclid(7);
                let remainder = days.rem_euclid(7);
                // 0 means legacy Snowflake behavior (ISO-like semantics)
                if (week_start == 0 && remainder > 0)
                    || (week_start != 0 && remainder >= week_start)
                {
                    weeks + 1
                } else {
                    weeks
                }
            }
        });
        ColumnarValue::Array(Arc::new(diff))
    }

    fn diff(diff_arr: &DurationNanosecondArray, coef: i64) -> ColumnarValue {
        let diff_arr: Int64Array = diff_arr.unary(|x| {
            let div = x / coef;
            if x % coef == 0 { div } else { div + 1 }
        });
        ColumnarValue::Array(Arc::new(diff_arr))
    }
}

impl ScalarUDFImpl for DateDiffFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "datediff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return internal_err!("function requires three arguments");
        }
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;

        if args.len() != 3 {
            return plan_err!("function requires three arguments");
        }
        let date_or_time_part = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
            _ => return plan_err!("Invalid unit type format"),
        };
        let len = match (&args[1], &args[2]) {
            (ColumnarValue::Array(arr1), _) => arr1.len(),
            (_, ColumnarValue::Array(arr2)) => arr2.len(),
            _ => 1,
        };
        let date_or_time_expr1 = broadcast_to_len(&args[1], len)?;
        let date_or_time_expr2 = broadcast_to_len(&args[2], len)?;

        match date_or_time_part.to_ascii_lowercase().as_str() {
            //should consider leap year (365-366 days)
            "year" | "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Year)
            }
            //should consider months 28-31 days
            "month" | "mm" | "mon" | "mons" | "months" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Month)
            }
            "day" | "d" | "dd" | "days" | "dayofmonth" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Day)
            }
            "week" | "w" | "wk" | "weekofyear" | "woy" | "wy" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Week)
            }
            //should consider months 28-31 days
            "quarter" | "q" | "qtr" | "qtrs" | "quarters" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Quarter)
            }
            "hour" | "h" | "hh" | "hr" | "hours" | "hrs" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Hour)
            }
            "minute" | "m" | "mi" | "min" | "minutes" | "mins" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Minute)
            }
            "second" | "s" | "sec" | "seconds" | "secs" => {
                self.date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Second)
            }
            "millisecond" | "ms" | "msec" | "milliseconds" => self.date_diff_func(
                &date_or_time_expr1,
                &date_or_time_expr2,
                DatePart::Millisecond,
            ),
            "microsecond" | "us" | "usec" | "microseconds" => self.date_diff_func(
                &date_or_time_expr1,
                &date_or_time_expr2,
                DatePart::Microsecond,
            ),
            "nanosecond" | "ns" | "nsec" | "nanosec" | "nsecond" | "nanoseconds" | "nanosecs" => {
                self.date_diff_func(
                    &date_or_time_expr1,
                    &date_or_time_expr2,
                    DatePart::Nanosecond,
                )
            }
            _ => plan_err!("Invalid date_or_time_part type")?,
        }
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn broadcast_to_len(val: &ColumnarValue, len: usize) -> Result<ArrayRef> {
    match val {
        ColumnarValue::Array(arr) => Ok(arr.clone()),
        ColumnarValue::Scalar(scalar) => Ok(scalar.to_array_of_size(len)?),
    }
}
