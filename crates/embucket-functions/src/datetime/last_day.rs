use crate::datetime::errors::CantCastToSnafu;
use crate::session_params::SessionParams;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Utc};
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
use snafu::OptionExt;
use std::any::Any;
use std::sync::Arc;

/// `LAST_DAY` SQL function
///
/// Returns the last day of the week/month/year/ for a given date or timestamp.
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
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LastDayFunc {
    signature: Signature,
    session_params: Arc<SessionParams>,
}

impl Default for LastDayFunc {
    fn default() -> Self {
        Self::new(Arc::new(SessionParams::default()))
    }
}

impl LastDayFunc {
    #[must_use]
    pub fn new(session_params: Arc<SessionParams>) -> Self {
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
            session_params,
        }
    }

    #[must_use]
    pub fn week_start(&self) -> u32 {
        self.session_params
            .get_property("week_start")
            .map_or_else(|| 0, |v| v.parse::<u32>().unwrap_or(0))
    }

    #[allow(clippy::as_conversions, clippy::cast_possible_truncation)]
    fn last_day(&self, date: &NaiveDateTime, date_part: &str) -> DFResult<NaiveDateTime> {
        let date = date.date();

        let new_date = match date_part.to_lowercase().as_str() {
            "day" | "d" | "dd" | "days" | "dayofmonth" => {
                date.and_hms_opt(0, 0, 0).context(CantCastToSnafu {
                    v: "native_datetime",
                })?
            }
            "week" | "w" | "wk" | "weekofyear" | "woy" | "wy" => {
                let week_start = self.week_start();
                // 0 means legacy Snowflake behavior (ISO-like semantics)
                let week_start_norm = if week_start == 0 {
                    0
                } else {
                    (week_start - 1) % 7
                };
                let last_day_num = (week_start_norm + 6) % 7;
                let weekday_num = date.weekday().num_days_from_monday();

                let days_until_week_end = (last_day_num + 7 - weekday_num) % 7;
                let last_day_of_week = date + Duration::days(days_until_week_end.into());
                last_day_of_week
                    .and_hms_opt(0, 0, 0)
                    .context(CantCastToSnafu {
                        v: "native_datetime",
                    })?
            }
            "month" | "mm" | "mon" | "mons" | "months" => {
                let year = date.year();
                let month = date.month();

                let (next_year, next_month) = if month == 12 {
                    (year + 1, 1)
                } else {
                    (year, month + 1)
                };

                let last_day = NaiveDate::from_ymd_opt(next_year, next_month, 1)
                    .context(CantCastToSnafu {
                        v: "native_datetime",
                    })?
                    .pred_opt()
                    .context(CantCastToSnafu {
                        v: "native_datetime",
                    })?;

                last_day.and_hms_opt(0, 0, 0).context(CantCastToSnafu {
                    v: "native_datetime",
                })?
            }
            "year" | "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" => {
                let year = date.year();
                NaiveDate::from_ymd_opt(year, 12, 31)
                    .context(CantCastToSnafu {
                        v: "native_datetime",
                    })?
                    .and_hms_opt(0, 0, 0)
                    .context(CantCastToSnafu {
                        v: "native_datetime",
                    })?
            }
            _ => return exec_err!("Unsupported date part: {}", date_part),
        };

        Ok(new_date)
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
        Ok(DataType::Date32)
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

        let arr = cast(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
        let arr = as_timestamp_nanosecond_array(&arr)?;

        let mut res = Date64Builder::with_capacity(arr.len());
        for row in arr {
            if let Some(ts) = row {
                let naive = DateTime::<Utc>::from_timestamp_nanos(ts).naive_utc();
                let last_day = self.last_day(&naive, &date_part)?;
                res.append_value(last_day.and_utc().timestamp_millis());
            } else {
                res.append_null();
            }
        }

        let res = res.finish();
        let arr = cast(&res, &DataType::Date32)?;
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
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
        ctx.register_udf(ScalarUDF::from(LastDayFunc::new(Arc::new(
            SessionParams::default(),
        ))));

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

        let sql = "SELECT last_day('2016-12-30'::date,'week')::date AS value;";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+------------+",
                "| value      |",
                "+------------+",
                "| 2017-01-01 |",
                "+------------+",
            ],
            &result
        );

        let sql = "SELECT 
            last_day('2024-05-08T23:39:20.123-07:00'::date,'year')::date AS v1,
            last_day('2024-05-08T23:39:20.123-07:00'::date,'y')::date AS v2;";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+------------+------------+",
                "| v1         | v2         |",
                "+------------+------------+",
                "| 2024-12-31 | 2024-12-31 |",
                "+------------+------------+",
            ],
            &result
        );
        Ok(())
    }
}
