use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Utc, Weekday};
use datafusion::arrow::array::{Array, Date64Builder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::types::logical_string;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// `NEXT_DAY` SQL function
///
/// Returns the date of the first specified day of week (DOW) that occurs after the input date.
///
/// Syntax: `NEXT_DAY( <date_or_timetamp_expr> , <dow_string> )`
///
/// Arguments:
/// - `date_or_timetamp_expr`: A date or timestamp value.
/// - `dow_string`: A string representing the day of the week (e.g., 'Monday', 'Tuesday', etc.).
///
/// Example: `SELECT NEXT_DAY('2025-05-06'::date, 'Friday')::date AS value;`
///
/// Returns:
/// - This function returns a value of type DATE, even if `date_or_timetamp_expr` is a timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NextDayFunc {
    signature: Signature,
}

impl Default for NextDayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NextDayFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    Exact(vec![DataType::Date32, DataType::Utf8]),
                    Exact(vec![DataType::Date64, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NextDayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "next_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(day))) = args[1].clone() else {
            return exec_err!("Second argument must be a string representing the day of the week");
        };

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut res = Date64Builder::with_capacity(arr.len());
        for i in 0..arr.len() {
            let v = ScalarValue::try_from_array(&arr, i)?
                .cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let ScalarValue::TimestampNanosecond(Some(ts), None) = v else {
                return exec_err!("First argument must be a timestamp with nanosecond precision");
            };
            let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();
            let next_day = next_day(&naive, &day.to_lowercase())?;

            res.append_value(next_day.and_utc().timestamp_millis());
        }

        let res = res.finish();
        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

#[allow(
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_lossless
)]
fn next_day(ndt: &NaiveDateTime, dow: &str) -> DFResult<NaiveDateTime> {
    let target_dow = if dow.starts_with("su") {
        Weekday::Sun
    } else if dow.starts_with("mo") {
        Weekday::Mon
    } else if dow.starts_with("tu") {
        Weekday::Tue
    } else if dow.starts_with("we") {
        Weekday::Wed
    } else if dow.starts_with("th") {
        Weekday::Thu
    } else if dow.starts_with("fr") {
        Weekday::Fri
    } else if dow.starts_with("sa") {
        Weekday::Sat
    } else {
        return exec_err!("Invalid day of week: {}", dow);
    };

    let current_dow = ndt.date().weekday();

    let mut days_to_add =
        (target_dow.num_days_from_sunday() + 7 - current_dow.num_days_from_sunday()) % 7;

    if days_to_add == 0 {
        days_to_add = 7;
    }

    Ok(*ndt + Duration::days(days_to_add as i64))
}

crate::macros::make_udf_function!(NextDayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(NextDayFunc::new()));

        let sql = "SELECT next_day('2025-05-06'::date, 'Friday')::date AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2025-05-09 |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
