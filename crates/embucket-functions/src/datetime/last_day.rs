use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
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

/// `LAST_DAY` SQL function
///
/// Returns the last day of the day/month/year for a given date or timestamp.
///
/// Syntax: `LAST_DAY(<date_or_timestamp>, <date_part>)`
///
/// Arguments:
/// - `date_or_timestamp`: A date or timestamp value.
/// - `date_part`: An optional string indicating the part of the date to consider.
///
/// Example: `SELECT LAST_DAY('2025-05-08T23:39:20.123-07:00'::date) AS value;`
///
/// Returns:
/// - Returns a date representing the last day of the specified part (day, month, or year).

#[derive(Debug)]
pub struct LastDayFunc {
    signature: Signature,
}

impl Default for LastDayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LastDayFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    Coercible(vec![Coercion::new_exact(TypeSignatureClass::Timestamp)]),
                    Exact(vec![DataType::Date32, DataType::Utf8]),
                    Exact(vec![DataType::Date32]),
                    Exact(vec![DataType::Date64, DataType::Utf8]),
                    Exact(vec![DataType::Date64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LastDayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "last_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date64)
    }

    #[allow(
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::cast_possible_truncation
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let date_part = if args.len() == 2 {
            let ColumnarValue::Scalar(ScalarValue::Utf8(Some(date_part))) = args[1].clone() else {
                return exec_err!("function requires the second argument to be a scalar");
            };
            date_part
        } else {
            "month".to_string()
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
            let last_day = last_day(&naive, &date_part)?;

            res.append_value(last_day.and_utc().timestamp_millis());
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
    clippy::cast_possible_truncation
)]
fn last_day(date: &NaiveDateTime, date_part: &str) -> DFResult<NaiveDateTime> {
    let date = date.date();

    let new_date = match date_part.to_lowercase().as_str() {
        "day" => date.and_hms_opt(0, 0, 0).unwrap(),
        "month" => {
            let year = date.year();
            let month = date.month();

            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };

            let last_day = NaiveDate::from_ymd_opt(next_year, next_month, 1)
                .unwrap()
                .pred_opt()
                .unwrap();

            last_day.and_hms_opt(0, 0, 0).unwrap()
        }
        "year" => {
            let year = date.year();
            NaiveDate::from_ymd_opt(year, 12, 31)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        }
        _ => return exec_err!("Unsupported date part: {}", date_part),
    };

    Ok(new_date)
}

crate::macros::make_udf_function!(LastDayFunc);
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(LastDayFunc::new()));

        let sql = "SELECT last_day('2025-05-08T23:39:20.123-07:00'::date)::date AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2025-05-31 |",
                "+------------+",
            ],
            &result
        );

        let sql = "SELECT last_day('2024-05-08T23:39:20.123-07:00'::date,'year')::date AS value;";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2024-12-31 |",
                "+------------+",
            ],
            &result
        );
        Ok(())
    }
}
