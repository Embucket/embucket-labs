use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Utc, Weekday};
use datafusion::arrow::array::Date64Builder;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::cast::as_timestamp_nanosecond_array;
use datafusion_common::types::logical_string;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// `PREVIOUS_DAY` SQL function
///
/// Returns the date of the first specified day of week (DOW) that occurs before the input date.
///
/// Syntax: `PREVIOUS_DAY( <date_or_timetamp_expr> , <dow_string> )`
///
/// Arguments:
/// - `date_or_timetamp_expr`: A date or timestamp value.
/// - `dow_string`: A string representing the day of the week (e.g., 'Monday', 'Tuesday', etc.).
///
/// Example: `SELECT PREVIOUS_DAY('2025-05-06'::date, 'Friday')::date AS value;`
///
/// Returns:
/// - This function returns a value of type DATE, even if `date_or_timetamp_expr` is a timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PreviousDayFunc {
    signature: Signature,
}

impl Default for PreviousDayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl PreviousDayFunc {
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

impl ScalarUDFImpl for PreviousDayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "previous_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date32)
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

        let arr = cast(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
        let arr = as_timestamp_nanosecond_array(&arr)?;

        let mut res = Date64Builder::with_capacity(arr.len());
        for row in arr {
            if let Some(ts) = row {
                let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();
                let prev_day = prev_day(&naive, &day.to_lowercase())?;
                res.append_value(prev_day.and_utc().timestamp_millis());
            } else {
                res.append_null();
            }
        }

        let res = res.finish();
        let arr = cast(&res, &DataType::Date32)?;
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

#[allow(
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_lossless
)]
fn prev_day(ndt: &NaiveDateTime, dow: &str) -> DFResult<NaiveDateTime> {
    let target_dow = match dow.chars().take(2).collect::<String>().as_str() {
        "mo" => Weekday::Mon,
        "tu" => Weekday::Tue,
        "we" => Weekday::Wed,
        "th" => Weekday::Thu,
        "fr" => Weekday::Fri,
        "sa" => Weekday::Sat,
        _ => return exec_err!("Invalid day of week: {}", dow),
    };

    let current_dow = ndt.date().weekday();

    let mut days_to_subtract =
        (current_dow.num_days_from_sunday() + 7 - target_dow.num_days_from_sunday()) % 7;

    if days_to_subtract == 0 {
        days_to_subtract = 7;
    }

    Ok(*ndt - Duration::days(days_to_subtract as i64))
}

crate::macros::make_udf_function!(PreviousDayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(PreviousDayFunc::new()));

        let sql = "SELECT previous_day('2025-05-06'::date, 'Friday')::date AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2025-05-02 |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
