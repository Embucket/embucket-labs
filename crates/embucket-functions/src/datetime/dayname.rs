use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// `DAYNAME` SQL function
///
/// Extracts the three-letter day-of-week name from the specified date or timestamp.
///
/// Syntax: `DAYNAME(<date_or_timestamp>)`
///
/// Arguments:
/// - `date_or_timestamp`: A date or timestamp value.
///
/// Example: `SELECT dayname('2025-05-08T23:39:20.123-07:00'::timestamp) AS value;`
///
/// Returns:
/// - Returns a string representing the three-letter day-of-week name (e.g., "Mon", "Tue", "Wed", etc.).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DayNameFunc {
    signature: Signature,
}

impl Default for DayNameFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DayNameFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![Coercion::new_exact(TypeSignatureClass::Timestamp)]),
                    Exact(vec![DataType::Date32]),
                    Exact(vec![DataType::Date64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DayNameFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "dayname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(clippy::as_conversions, clippy::cast_possible_truncation)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut res = StringBuilder::with_capacity(arr.len(), 1024);
        for i in 0..arr.len() {
            let v = ScalarValue::try_from_array(&arr, i)?
                .cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let ScalarValue::TimestampNanosecond(Some(ts), None) = v else {
                return exec_err!("First argument must be a timestamp with nanosecond precision");
            };
            let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();
            res.append_value(dayname(&naive));
        }

        let res = res.finish();
        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

fn dayname(date: &NaiveDateTime) -> String {
    match date.weekday() {
        chrono::Weekday::Mon => "Mon".to_string(),
        chrono::Weekday::Tue => "Tue".to_string(),
        chrono::Weekday::Wed => "Wed".to_string(),
        chrono::Weekday::Thu => "Thu".to_string(),
        chrono::Weekday::Fri => "Fri".to_string(),
        chrono::Weekday::Sat => "Sat".to_string(),
        chrono::Weekday::Sun => "Sun".to_string(),
    }
}

crate::macros::make_udf_function!(DayNameFunc);
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(DayNameFunc::new()));

        let sql = "SELECT dayname('2025-05-08T23:39:20.123-07:00'::date) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "| Fri   |",
                "+-------+",
            ],
            &result
        );

        Ok(())
    }
}
