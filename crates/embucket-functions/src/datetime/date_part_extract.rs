use chrono::{DateTime, Datelike, Timelike, Utc};
use datafusion::arrow::array::{Array, Int64Builder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Interval {
    Year,
    YearOfWeek,
    YearOfWeekIso,
    Day,
    DayOfMonth,
    DayOfWeek,
    DayOfWeekIso,
    DayOfYear,
    Week,
    WeekOfYear,
    WeekIso,
    Month,
    Quarter,
    Hour,
    Minute,
    Second,
}

#[derive(Debug)]
pub struct DatePartExtractFunc {
    signature: Signature,
    interval: Interval,
}

impl Default for DatePartExtractFunc {
    fn default() -> Self {
        Self::new(Interval::Year)
    }
}

impl DatePartExtractFunc {
    #[must_use]
    pub fn new(interval: Interval) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![Coercion::new_exact(TypeSignatureClass::Timestamp)]),
                    Exact(vec![DataType::Date32]),
                    Exact(vec![DataType::Date64]),
                ],
                Volatility::Immutable,
            ),
            interval,
        }
    }
}

impl ScalarUDFImpl for DatePartExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        match self.interval {
            Interval::Year => "year",
            Interval::YearOfWeek => "yearofweek",
            Interval::YearOfWeekIso => "yearofweekiso",
            Interval::Day => "day",
            Interval::DayOfMonth => "dayofmonth",
            Interval::DayOfWeek => "dayofweek",
            Interval::DayOfWeekIso => "dayofweekiso",
            Interval::DayOfYear => "dayofyear",
            Interval::Week => "week",
            Interval::WeekOfYear => "weekofyear",
            Interval::WeekIso => "weekiso",
            Interval::Month => "month",
            Interval::Quarter => "quarter",
            Interval::Hour => "hour",
            Interval::Minute => "minute",
            Interval::Second => "second",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    #[allow(
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_lossless
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut res = Int64Builder::with_capacity(arr.len());
        for i in 0..arr.len() {
            let v = ScalarValue::try_from_array(&arr, i)?
                .cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let ScalarValue::TimestampNanosecond(Some(ts), None) = v else {
                return exec_err!("First argument must be a timestamp with nanosecond precision");
            };
            let naive = DateTime::<Utc>::from_timestamp_nanos(ts);
            let value = match self.interval {
                Interval::Year => naive.date_naive().year(),
                Interval::YearOfWeekIso | Interval::YearOfWeek => {
                    naive.date_naive().iso_week().year()
                }
                Interval::Day | Interval::DayOfMonth => naive.date_naive().day() as i32 - 1,
                Interval::DayOfWeek => naive.date_naive().weekday().num_days_from_monday() as i32, // 0 = Monday, 6 = Sunday
                Interval::DayOfYear => naive.date_naive().ordinal() as i32 - 1, // 1..=365/366
                Interval::Week => (naive.date_naive().ordinal() - 1) as i32 / 7 + 1,
                Interval::WeekOfYear | Interval::WeekIso => {
                    naive.date_naive().iso_week().week() as i32
                } // ISO week number
                Interval::Month => naive.date_naive().month() as i32, // 1..=12
                Interval::Quarter => ((naive.date_naive().month() - 1) / 3 + 1) as i32, // 1..=4
                Interval::DayOfWeekIso => naive.date_naive().weekday().number_from_monday() as i32,
                Interval::Hour => naive.hour() as i32,
                Interval::Minute => naive.minute() as i32,
                Interval::Second => naive.second() as i32,
            };

            res.append_value(value as i64);
        }

        let res = res.finish();
        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(Interval::Year)));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::YearOfWeek,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(Interval::Day)));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::YearOfWeekIso,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::YearOfWeekIso,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::DayOfMonth,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::DayOfWeek,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::DayOfWeekIso,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::DayOfYear,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(Interval::Week)));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::WeekOfYear,
        )));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(Interval::WeekIso)));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(Interval::Month)));
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(Interval::Quarter)));

        let sql = r#"SELECT '2025-04-11T23:39:20.123-07:00'::TIMESTAMP AS tstamp,
       YEAR('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "YEAR",
       QUARTER('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "QUARTER OF YEAR",
       MONTH('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "MONTH",
       DAY('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "DAY",
       DAYOFMONTH('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF MONTH",
       DAYOFYEAR('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF YEAR";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------+------+-----------------+-------+-----+--------------+-------------+",
                "| tstamp                  | YEAR | QUARTER OF YEAR | MONTH | DAY | DAY OF MONTH | DAY OF YEAR |",
                "+-------------------------+------+-----------------+-------+-----+--------------+-------------+",
                "| 2025-04-12T06:39:20.123 | 2025 | 2               | 4     | 11  | 11           | 101         |",
                "+-------------------------+------+-----------------+-------+-----+--------------+-------------+",
            ],
            &result
        );

        let sql = r#"SELECT '2016-01-02T23:39:20.123-07:00'::TIMESTAMP AS tstamp,
        WEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "WEEK",
       WEEKISO('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)       AS "WEEK ISO",
       WEEKOFYEAR('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)    AS "WEEK OF YEAR",
       YEAROFWEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)    AS "YEAR OF WEEK",
       YEAROFWEEKISO('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "YEAR OF WEEK ISO""#;
        let result = ctx.sql(sql).await?.collect().await?;

        print_batches(&result)?;
        Ok(())
    }
}
