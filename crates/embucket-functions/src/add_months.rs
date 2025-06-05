use crate::to_time::ToTimeFunc;
use chrono::{
    DateTime, Datelike, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, ParseError,
    Timelike, Utc,
};
use datafusion::arrow::array::builder::Time64NanosecondBuilder;
use datafusion::arrow::array::types::Time64NanosecondType;
use datafusion::arrow::array::{Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::{DataFusionError, ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct AddMonths {
    signature: Signature,
}

impl Default for AddMonths {
    fn default() -> Self {
        Self::new()
    }
}

impl AddMonths {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                    ]),
                    Exact(vec![DataType::Date32, DataType::Int64]),
                    Exact(vec![DataType::Date64, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AddMonths {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "add_months"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let ColumnarValue::Scalar(ScalarValue::Int64(Some(to_add))) = args[1].clone() else {
            return exec_err!(
                "Second argument must be a scalar Int64 value representing months to add"
            );
        };

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut res = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            let v = ScalarValue::try_from_array(&arr, i)?
                .cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let ScalarValue::TimestampNanosecond(Some(ts), None) = v else {
                return exec_err!("First argument must be a timestamp with nanosecond precision");
            };

            let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();
            let new_naive = add_months(&naive, to_add as i32)
                .ok_or_else(|| DataFusionError::Execution("can't parse date".to_string()))?;

            let v = new_naive
                .and_utc()
                .timestamp_nanos_opt()
                .expect("Timestamp out of range");
            let tsv = ScalarValue::TimestampNanosecond(Some(v), None);
            res.push(tsv.cast_to(arr.data_type())?);
        }

        let arr = ScalarValue::iter_to_array(res)?;

        Ok(if arr.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(arr))
        })
    }
}

fn add_months(dt: &NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    let date = dt.date();
    let time = dt.time();

    let year = date.year();
    let month = date.month() as i32;

    let total_months = year * 12 + (month - 1) + months;
    let new_year = total_months / 12;
    let new_month = (total_months % 12) + 1;

    let is_last_day = date.day() == last_day_of_month(year, month as u32)?;

    let new_day = if is_last_day {
        last_day_of_month(new_year, new_month as u32)?
    } else {
        date.day()
            .min(last_day_of_month(new_year, new_month as u32)?)
    };

    NaiveDate::from_ymd_opt(new_year, new_month as u32, new_day).map(|d| d.and_time(time))
}

fn last_day_of_month(year: i32, month: u32) -> Option<u32> {
    let first_of_next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }?;

    Some(first_of_next_month.pred_opt()?.day())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_indexing() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(AddMonths::new()));

        let sql = "SELECT add_months('2022-01-01 11:30:00'::timestamp AT TIME ZONE 'Europe/Brussels',-1) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------+",
                "| value                     |",
                "+---------------------------+",
                "| 2021-12-01T10:30:00+01:00 |",
                "+---------------------------+",
            ],
            &result
        );

        let sql = "SELECT add_months('2022-01-01 11:30:00'::date,1) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2022-02-01 |",
                "+------------+",
            ],
            &result
        );

        let sql = "SELECT add_months('2016-01-31'::date,1) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2016-02-29 |",
                "+------------+",
            ],
            &result
        );

        let sql = "SELECT add_months('2016-02-29'::date,1) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2016-03-31 |",
                "+------------+",
            ],
            &result
        );
        Ok(())
    }
}
