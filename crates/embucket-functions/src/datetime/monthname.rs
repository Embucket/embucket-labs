use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::types::logical_date;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// `MONTHNAME` SQL function
///
/// Extracts the three-letter month name from the specified date or timestamp.
///
/// Syntax: `MONTHNAME(<date_or_timestamp>)`
///
/// Arguments:
/// - `date_or_timestamp`: A date or timestamp value.
///
/// Example: `SELECT monthname('2025-05-08T23:39:20.123-07:00'::timestamp) AS value;`
///
/// Returns:
/// - Returns a string representing the three-letter month name
#[derive(Debug)]
pub struct MonthNameFunc {
    signature: Signature,
}

impl Default for MonthNameFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MonthNameFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![Coercion::new_exact(TypeSignatureClass::Timestamp)]),
                    Coercible(vec![Coercion::new_exact(TypeSignatureClass::Native(
                        logical_date(),
                    ))]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MonthNameFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "monthname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::cast_possible_truncation
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        match args[0].clone() {
            ColumnarValue::Array(arr) => {
                let mut res = StringBuilder::with_capacity(arr.len(), 1024);
                for i in 0..arr.len() {
                    let v = ScalarValue::try_from_array(&arr, i)?
                        .cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
                    let ScalarValue::TimestampNanosecond(Some(ts), None) = v else {
                        return exec_err!(
                            "First argument must be a timestamp with nanosecond precision"
                        );
                    };
                    let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();
                    res.append_value(format!("{}", naive.format("%b")));
                }

                Ok(ColumnarValue::Array(Arc::new(res.finish())))
            }
            ColumnarValue::Scalar(v) => {
                let v = v.cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;

                let ScalarValue::TimestampNanosecond(Some(ts), None) = v else {
                    return exec_err!(
                        "First argument must be a timestamp with nanosecond precision"
                    );
                };
                let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    naive.format("%b").to_string(),
                ))))
            }
        }
    }
}

crate::macros::make_udf_function!(MonthNameFunc);
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(MonthNameFunc::new()));

        let sql = "SELECT monthname('2025-06-08T23:39:20.123-07:00'::date) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "| Jun   |",
                "+-------+",
            ],
            &result
        );

        Ok(())
    }
}
