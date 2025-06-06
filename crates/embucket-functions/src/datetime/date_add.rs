use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::kernels::numeric::add_wrapping;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::TimeUnit::Nanosecond;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_common::types::{NativeType, logical_date, logical_int64, logical_string};
use datafusion_expr::Coercion;
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
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Timestamp,
                            vec![TypeSignatureClass::Native(logical_string())],
                            NativeType::Timestamp(Nanosecond, None),
                        ),
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
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

// dateadd SQL function
// Syntax: `DATEADD(<date_or_time_part>, <value>, <date_or_time_expr>)`
// - <date_or_time_part>: This indicates the units of time that you want to add.
// For example if you want to add two days, then specify day. This unit of measure must be one of the values listed in Supported date and time parts.
// - <value>: This is the number of units of time that you want to add.
// For example, if the units of time is day, and you want to add two days, specify 2. If you want to subtract two days, specify -2.
// - <date_or_time_expr>: Must evaluate to a date, time, or timestamp.
// This is the date, time, or timestamp to which you want to add.
// For example, if you want to add two days to August 1, 2024, then specify '2024-08-01'::DATE.
// If the data type is TIME, then the date_or_time_part must be in units of hours or smaller, not days or bigger.
// If the input data type is DATE, and the date_or_time_part is hours or smaller, the input value will not be rejected,
// but instead will be treated as a TIMESTAMP with hours, minutes, seconds, and fractions of a second all initially set to 0 (e.g. midnight on the specified date).
//
// Note: `dateadd` returns
// If date_or_time_expr is a time, then the return data type is a time.
// If date_or_time_expr is a timestamp, then the return data type is a timestamp.
// If date_or_time_expr is a date:
// - If date_or_time_part is day or larger (for example, month, year), the function returns a DATE value.
// - If date_or_time_part is smaller than a day (for example, hour, minute, second), the function returns a TIMESTAMP_NTZ value, with 00:00:00.000 as the starting time for the date.
// Usage notes:
// - When date_or_time_part is year, quarter, or month (or any of their variations),
// if the result month has fewer days than the original day of the month, the result day of the month might be different from the original day.
// Examples
// - dateadd(day, 30, CAST('2024-12-26' AS TIMESTAMP))
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
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_date_add_days_timestamp() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("days")))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5i64))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(1_736_168_400_000_000_i64),
                Some(Arc::from(String::from("+00").into_boxed_str())),
            )),
        ];
        let fn_args = ScalarFunctionArgs {
            args,
            number_rows: 0,
            return_type: &datafusion::arrow::datatypes::DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from(String::from("+00").into_boxed_str())),
            ),
        };
        match DateAddFunc::new().invoke_with_args(fn_args) {
            Ok(ColumnarValue::Scalar(result)) => {
                let expected = ScalarValue::TimestampMicrosecond(
                    Some(1_736_600_400_000_000_i64),
                    Some(Arc::from(String::from("+00").into_boxed_str())),
                );
                assert_eq!(&result, &expected, "date_add created a wrong value");
            }
            _ => panic!("Conversion failed"),
        }
    }
    #[test]
    fn test_date_add_days_timestamp_array() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("days")))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5i64))),
            ColumnarValue::Array(
                ScalarValue::TimestampMicrosecond(
                    Some(1_736_168_400_000_000_i64),
                    Some(Arc::from(String::from("+00").into_boxed_str())),
                )
                .to_array()
                .unwrap(),
            ),
        ];
        let fn_args = ScalarFunctionArgs {
            args,
            number_rows: 0,
            return_type: &datafusion::arrow::datatypes::DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from(String::from("+00").into_boxed_str())),
            ),
        };
        match DateAddFunc::new().invoke_with_args(fn_args) {
            Ok(ColumnarValue::Array(result)) => {
                let expected = ScalarValue::TimestampMicrosecond(
                    Some(1_736_600_400_000_000_i64),
                    Some(Arc::from(String::from("+00").into_boxed_str())),
                )
                .to_array()
                .unwrap();
                assert_eq!(&result, &expected, "date_add created a wrong value");
            }
            _ => panic!("Conversion failed"),
        }
    }
    #[test]
    fn test_date_add_days_timestamp_array_multiple_values() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("days")))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5i64))),
            ColumnarValue::Array(
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(1_736_168_400_000_000_i64),
                    Some(Arc::from(String::from("+00").into_boxed_str())),
                ))
                .to_array(2)
                .unwrap(),
            ),
        ];
        let fn_args = ScalarFunctionArgs {
            args,
            number_rows: 0,
            return_type: &datafusion::arrow::datatypes::DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from(String::from("+00").into_boxed_str())),
            ),
        };
        match DateAddFunc::new().invoke_with_args(fn_args) {
            Ok(ColumnarValue::Array(result)) => {
                let expected = ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(1_736_600_400_000_000_i64),
                    Some(Arc::from(String::from("+00").into_boxed_str())),
                ))
                .to_array(2)
                .unwrap();
                assert_eq!(&result, &expected, "date_add created a wrong value");
            }
            _ => panic!("Conversion failed"),
        }
    }
}
