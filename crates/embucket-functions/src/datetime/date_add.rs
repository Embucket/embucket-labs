use crate::datetime_errors::InvalidArgumentSnafu;
use arrow_schema::TimeUnit;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder};
use datafusion::arrow::compute::kernels::numeric::add_wrapping;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use datafusion::scalar::ScalarValue;
use datafusion_common::cast::{as_float64_array, as_int64_array};
use datafusion_common::utils::take_function_args;
use datafusion_expr::ScalarFunctionArgs;
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

    fn add_years(dates: &Arc<dyn Array>, years: &Int64Array) -> Result<ArrayRef> {
        let intervals: Vec<ScalarValue> = years
            .iter()
            .map(|opt_y| {
                ScalarValue::new_interval_ym(i32::try_from(opt_y.unwrap_or(0)).unwrap_or(0), 0)
            })
            .collect();
        let interval_array =
            ColumnarValue::Array(ScalarValue::iter_to_array(intervals)?).to_array(dates.len())?;
        Ok(add_wrapping(dates, &interval_array)?)
    }

    fn add_months(
        dates: &Arc<dyn Array>,
        months: &Int64Array,
        multiplier: i64,
    ) -> Result<ArrayRef> {
        let intervals: Vec<ScalarValue> = months
            .iter()
            .map(|m| {
                ScalarValue::new_interval_ym(
                    0,
                    i32::try_from(m.unwrap_or(0) * multiplier).unwrap_or(0),
                )
            })
            .collect();
        let interval_array =
            ColumnarValue::Array(ScalarValue::iter_to_array(intervals)?).to_array(dates.len())?;
        Ok(add_wrapping(dates, &interval_array)?)
    }

    fn add_days(dates: &Arc<dyn Array>, days: &Int64Array, multiplier: i64) -> Result<ArrayRef> {
        let intervals: Vec<ScalarValue> = days
            .iter()
            .map(|opt_d| {
                ScalarValue::new_interval_dt(
                    i32::try_from(opt_d.unwrap_or(0) * multiplier).unwrap_or(0),
                    0,
                )
            })
            .collect();
        let interval_array =
            ColumnarValue::Array(ScalarValue::iter_to_array(intervals)?).to_array(dates.len())?;
        Ok(add_wrapping(dates, &interval_array)?)
    }

    fn add_nanoseconds(
        dates: &Arc<dyn Array>,
        nanos: &Int64Array,
        multiplier: i64,
    ) -> Result<ArrayRef> {
        let intervals: Vec<ScalarValue> = nanos
            .iter()
            .map(|opt_ns| ScalarValue::new_interval_mdn(0, 0, opt_ns.unwrap_or(0) * multiplier))
            .collect();
        let interval_array =
            ColumnarValue::Array(ScalarValue::iter_to_array(intervals)?).to_array(dates.len())?;
        Ok(add_wrapping(dates, &interval_array)?)
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
///
/// Usage notes:
/// - When `date_or_time_part` is year, quarter, or month (or any of their variations),
///   if the result month has fewer days than the original day of the month, the result day of the month might be different from the original day.
///
/// Examples
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
            return InvalidArgumentSnafu {
                description: "function requires three arguments",
            }
            .fail()?;
        }
        Ok(arg_types[2].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [units, value, expr] = take_function_args(self.name(), arg_types)?;
        let units = match units {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                DataType::Utf8
            }
            other => {
                return InvalidArgumentSnafu {
                    description: format!("First argument must be a string, but found {other:?}"),
                }
                .fail()?;
            }
        };

        let value = match value.clone() {
            v if v.is_integer() => DataType::Int64,
            v if v.is_numeric() => DataType::Float64,
            other => {
                return InvalidArgumentSnafu {
                    description: format!("Second argument must be a number, but found {other:?}"),
                }
                .fail()?;
            }
        };

        let expr =
            match expr {
                v if v.is_temporal() => v.clone(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
                _ => return InvalidArgumentSnafu {
                    description: format!(
                        "Third argument must be date, time, timestamp or string, but found {expr:?}"
                    ),
                }
                .fail()?,
            };

        // `value` is the number of units of time that you want to add.
        // For example, if the units of time is day, and you want to add two days, specify 2.
        // If you want to subtract two days, specify -2. It should be an integer.
        Ok(vec![units, value, expr])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return InvalidArgumentSnafu {
                description: "function requires three arguments",
            }
            .fail()?;
        }
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let date_or_time_part = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
            _ => {
                return InvalidArgumentSnafu {
                    description: "Invalid unit type format",
                }
                .fail()?;
            }
        };

        let values_array = args[1].clone().into_array(number_rows)?;
        let values = match values_array.data_type() {
            DataType::Int64 => as_int64_array(&values_array)?,
            DataType::Float64 => {
                let float_array = as_float64_array(&values_array)?;
                let mut builder = Int64Builder::with_capacity(float_array.len());
                for f in float_array.values() {
                    builder.append_option(f.round().to_i64());
                }
                &builder.finish()
            }
            _ => {
                return InvalidArgumentSnafu {
                    description: "Second argument must be numeric",
                }
                .fail()?;
            }
        };
        let date_or_time_expr = args[2].clone().into_array(number_rows)?;

        // there shouldn't be overflows
        let result = match date_or_time_part.as_str() {
            //should consider leap year (365-366 days)
            "year" | "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" => {
                Self::add_years(&date_or_time_expr, values)
            }
            //should consider months 28-31 days
            "month" | "mm" | "mon" | "mons" | "months" => {
                Self::add_months(&date_or_time_expr, values, 1)
            }
            "day" | "d" | "dd" | "days" | "dayofmonth" => {
                Self::add_days(&date_or_time_expr, values, 1)
            }
            "week" | "w" | "wk" | "weekofyear" | "woy" | "wy" => {
                Self::add_days(&date_or_time_expr, values, 7)
            }
            //should consider months 28-31 days
            "quarter" | "q" | "qtr" | "qtrs" | "quarters" => {
                Self::add_months(&date_or_time_expr, values, 3)
            }
            "hour" | "h" | "hh" | "hr" | "hours" | "hrs" => {
                Self::add_nanoseconds(&date_or_time_expr, values, 3_600_000_000_000)
            }
            "minute" | "m" | "mi" | "min" | "minutes" | "mins" => {
                Self::add_nanoseconds(&date_or_time_expr, values, 60_000_000_000)
            }
            "second" | "s" | "sec" | "seconds" | "secs" => {
                Self::add_nanoseconds(&date_or_time_expr, values, 1_000_000_000)
            }
            "millisecond" | "ms" | "msec" | "milliseconds" => {
                Self::add_nanoseconds(&date_or_time_expr, values, 1_000_000)
            }
            "microsecond" | "us" | "usec" | "microseconds" => {
                Self::add_nanoseconds(&date_or_time_expr, values, 1000)
            }
            "nanosecond" | "ns" | "nsec" | "nanosec" | "nsecond" | "nanoseconds" | "nanosecs"
            | "nseconds" => Self::add_nanoseconds(&date_or_time_expr, values, 1),
            _ => InvalidArgumentSnafu {
                description: "Invalid date_or_time_part type",
            }
            .fail()?,
        };
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
        let sql = "SELECT DATEADD('days', 5, 1735678800::TIMESTAMP) as res";
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
        let sql = "WITH vals AS (SELECT * FROM VALUES (1735678800),(1735678800) AS t(num))
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
