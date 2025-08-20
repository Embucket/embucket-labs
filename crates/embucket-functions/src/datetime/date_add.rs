use crate::datetime::is_datetime_like;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::kernels::numeric::add_wrapping;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use datafusion::scalar::ScalarValue;
use datafusion_common::utils::take_function_args;
use rust_decimal::prelude::ToPrimitive;
use std::any::Any;
use std::sync::Arc;

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

#[allow(clippy::unnecessary_wraps)]
impl DateAddFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Immutable),
            aliases: vec![
                String::from("date_add"),
                String::from("time_add"),
                String::from("timeadd"),
                String::from("timestamp_add"),
                String::from("timestampadd"),
            ],
        }
    }

    fn add_years(val: &Arc<dyn Array>, years: i64) -> Result<ArrayRef> {
        let years = ColumnarValue::Scalar(ScalarValue::new_interval_ym(
            i32::try_from(years).unwrap_or(0),
            0,
        ))
        .to_array(val.len())?;
        Ok(add_wrapping(&val, &years)?)
    }
    fn add_months(val: &Arc<dyn Array>, months: i64) -> Result<ArrayRef> {
        let months = ColumnarValue::Scalar(ScalarValue::new_interval_ym(
            0,
            i32::try_from(months).unwrap_or(0),
        ))
        .to_array(val.len())?;
        Ok(add_wrapping(&val, &months)?)
    }
    fn add_days(val: &Arc<dyn Array>, days: i64) -> Result<ArrayRef> {
        let days = ColumnarValue::Scalar(ScalarValue::new_interval_dt(
            i32::try_from(days).unwrap_or(0),
            0,
        ))
        .to_array(val.len())?;
        Ok(add_wrapping(&val, &days)?)
    }

    fn add_nanoseconds(val: &Arc<dyn Array>, nanoseconds: i64) -> Result<ArrayRef> {
        let nanoseconds = ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, 0, nanoseconds))
            .to_array(val.len())?;
        Ok(add_wrapping(&val, &nanoseconds)?)
    }
}

/// dateadd SQL function
/// Syntax: `DATEADD(<date_or_time_part>, <value>, <date_or_time_expr>)`
/// - <`date_or_time_part`>: This indicates the units of time that you want to add.
///   For example if you want to add two days, then specify day. This unit of measure must be one of the values listed in Supported date and time parts.
/// - <value>: This is the number of units of time that you want to add.
///   For example, if the units of time is day, and you want to add two days, specify 2. If you want to subtract two days, specify -2.
/// - <`date_or_time_expr`>: Must evaluate to a date, time, or timestamp.
///   This is the date, time, or timestamp to which you want to add.
///   For example, if you want to add two days to August 1, 2024, then specify '2024-08-01'`::DATE`.
///   If the data type is TIME, then the `date_or_time_part` must be in units of hours or smaller, not days or bigger.
///   If the input data type is DATE, and the `date_or_time_part` is hours or smaller, the input value will not be rejected,
///   but instead will be treated as a TIMESTAMP with hours, minutes, seconds, and fractions of a second all initially set to 0 (e.g. midnight on the specified date).
///
/// Note: `dateadd` returns
/// - If `date_or_time_expr` is a time, then the return data type is a time.
/// - If `date_or_time_expr` is a timestamp, then the return data type is a timestamp.
/// - If `date_or_time_expr` is a date:
/// - If `date_or_time_part` is day or larger (for example, month, year), the function returns a DATE value.
/// - If `date_or_time_part` is smaller than a day (for example, hour, minute, second), the function returns a `TIMESTAMP_NTZ` value, with 00:00:00.000 as the starting time for the date.
///   Usage notes:
/// - When `date_or_time_part` is year, quarter, or month (or any of their variations),
///   if the result month has fewer days than the original day of the month, the result day of the month might be different from the original day.
///   Examples
/// - dateadd(day, 30, CAST('2024-12-26' AS TIMESTAMP))
impl ScalarUDFImpl for DateAddFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [units, value, expr] = take_function_args(self.name(), arg_types)?;
        let units = match units {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Null => DataType::Utf8,
            other => {
                return plan_err!("First argument must be a string, but found {:?}", other);
            }
        };
        if !is_datetime_like(expr) {
            return plan_err!("third arguments must be date, time, timestamp or string");
        }

        let value = match value.clone() {
            v if v.is_integer() => DataType::Int64,
            v if v.is_numeric() => DataType::Float64,
            other => {
                return plan_err!("Second argument must be a number, but found {:?}", other);
            }
        };

        // `value` is the number of units of time that you want to add.
        // For example, if the units of time is day, and you want to add two days, specify 2.
        // If you want to subtract two days, specify -2. It should be an integer.
        Ok(vec![units, value, expr.clone()])
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 3 {
            return plan_err!("function requires three arguments");
        }
        let date_or_time_part = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
            _ => return plan_err!("Invalid unit type format"),
        };

        let value = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(val))) => *val,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) => val
                .round()
                .to_i64()
                .map_or_else(|| plan_err!("Value out of range"), Ok)?,
            _ => return plan_err!("Invalid value type"),
        };
        let (is_scalar, date_or_time_expr) = match &args[2] {
            ColumnarValue::Scalar(val) => (true, val.to_array()?),
            ColumnarValue::Array(array) => (false, array.clone()),
        };
        //there shouldn't be overflows
        let result = match date_or_time_part.as_str() {
            //should consider leap year (365-366 days)
            "year" | "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" => {
                Self::add_years(&date_or_time_expr, value)
            }
            //should consider months 28-31 days
            "month" | "mm" | "mon" | "mons" | "months" => {
                Self::add_months(&date_or_time_expr, value)
            }
            "day" | "d" | "dd" | "days" | "dayofmonth" => Self::add_days(&date_or_time_expr, value),
            "week" | "w" | "wk" | "weekofyear" | "woy" | "wy" => {
                Self::add_days(&date_or_time_expr, value * 7)
            }
            //should consider months 28-31 days
            "quarter" | "q" | "qtr" | "qtrs" | "quarters" => {
                Self::add_months(&date_or_time_expr, value * 3)
            }
            "hour" | "h" | "hh" | "hr" | "hours" | "hrs" => {
                Self::add_nanoseconds(&date_or_time_expr, value * 3_600_000_000_000)
            }
            "minute" | "m" | "mi" | "min" | "minutes" | "mins" => {
                Self::add_nanoseconds(&date_or_time_expr, value * 60_000_000_000)
            }
            "second" | "s" | "sec" | "seconds" | "secs" => {
                Self::add_nanoseconds(&date_or_time_expr, value * 1_000_000_000)
            }
            "millisecond" | "ms" | "msec" | "milliseconds" => {
                Self::add_nanoseconds(&date_or_time_expr, value * 1_000_000)
            }
            "microsecond" | "us" | "usec" | "microseconds" => {
                Self::add_nanoseconds(&date_or_time_expr, value * 1000)
            }
            "nanosecond" | "ns" | "nsec" | "nanosec" | "nsecond" | "nanoseconds" | "nanosecs"
            | "nseconds" => Self::add_nanoseconds(&date_or_time_expr, value),
            _ => plan_err!("Invalid date_or_time_part type"),
        };
        if is_scalar {
            let result = result.and_then(|array| ScalarValue::try_from_array(&array, 0));
            return result.map(ColumnarValue::Scalar);
        }
        result.map(ColumnarValue::Array)
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

crate::macros::make_udf_function!(DateAddFunc);
#[cfg(test)]
#[allow(clippy::unwrap_in_result, clippy::unwrap_used)]
mod tests {
    use super::DateAddFunc;
    use datafusion::common::Result as DFResult;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_date_add_days_timestamp() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(DateAddFunc::new()));
        let sql = r#"SELECT DATEADD('days', 5, 1735678800::TIMESTAMP) as res"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| res                 |",
                "+---------------------+",
                "| 2025-01-05T21:00:00 |",
                "+---------------------+",
            ],
            &result
        );

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(DateAddFunc::new()));
        let sql = r"
            WITH vals AS (SELECT * FROM VALUES (1735678800),(1735678800) AS t(num))
            SELECT 
                DATEADD('days', 5, num::TIMESTAMP) as c1,
                DATEADD('days', 5.6, num::TIMESTAMP) as c2
            FROM vals as res";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+---------------------+",
                "| c1                  | c2                  |",
                "+---------------------+---------------------+",
                "| 2025-01-05T21:00:00 | 2025-01-06T21:00:00 |",
                "| 2025-01-05T21:00:00 | 2025-01-06T21:00:00 |",
                "+---------------------+---------------------+",
            ],
            &result
        );
        Ok(())
    }
}
