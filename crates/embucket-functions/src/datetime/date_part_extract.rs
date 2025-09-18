use crate::datetime::errors::CantCastToSnafu;
use crate::session_params::SessionParams;
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc, Weekday};
use datafusion::arrow::array::{Array, Int64Builder};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_timestamp_nanosecond_array;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use snafu::OptionExt;
use std::any::Any;
use std::sync::Arc;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(EnumIter, Debug, PartialEq, Eq, Hash, Clone)]
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

/// `YEAR*` / `DAY*` / `WEEK*` / `MONTH` / `QUARTER` / `HOUR` / `MINUTE` / `SECOND` SQL function
///
/// Extracts a specific part of a date or timestamp.
///
/// These functions are alternatives to using the `DATE_PART` function with the equivalent date part (see Supported date and time parts).
///
/// Syntax: `YEAR(<date_or_timestamp>)`
///
/// Arguments:
/// - `date_or_timestamp`: A date or timestamp value.
///
/// Example: `SELECT YEAR('2025-05-08T23:39:20.123-07:00'::timestamp) AS value;`
///
/// Returns:
/// - Returns an integer representing the specified part of the date or timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DatePartExtractFunc {
    signature: Signature,
    interval: Interval,
    session_params: Arc<SessionParams>,
}

impl Default for DatePartExtractFunc {
    fn default() -> Self {
        Self::new(Interval::Year, Arc::new(SessionParams::default()))
    }
}

impl DatePartExtractFunc {
    #[must_use]
    pub fn new(interval: Interval, session_params: Arc<SessionParams>) -> Self {
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
            session_params,
        }
    }

    #[must_use]
    pub fn week_start(&self) -> usize {
        self.session_params
            .get_property("week_start")
            .map_or_else(|| 0, |v| v.parse::<usize>().unwrap_or(0))
    }

    #[must_use]
    pub fn week_of_year_policy(&self) -> usize {
        self.session_params
            .get_property("week_of_year_policy")
            .map_or_else(|| 0, |v| v.parse::<usize>().unwrap_or(0))
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
        let arr = cast_with_options(
            &arr,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            &CastOptions::default(),
        )?;
        let arr = as_timestamp_nanosecond_array(&arr)?;

        for v in arr {
            match v {
                None => res.append_null(),
                Some(ts) => {
                    let naive = DateTime::<Utc>::from_timestamp_nanos(ts);
                    let date = naive.date_naive();
                    let value = match self.interval {
                        Interval::Year => date.year(),
                        Interval::YearOfWeekIso => {
                            // Always use ISO year regardless of session settings
                            date.iso_week().year()
                        }
                        Interval::YearOfWeek => {
                            // Use session settings for year of week calculation
                            calculate_year_of_week(
                                date,
                                self.week_start(),
                                self.week_of_year_policy(),
                            )?
                        }
                        Interval::Day | Interval::DayOfMonth => date.day() as i32,
                        Interval::DayOfWeek => {
                            // Use session week_start setting
                            calculate_day_of_week(date, self.week_start())
                        }
                        Interval::DayOfWeekIso => {
                            // Always use ISO (Monday = 1, Sunday = 7) regardless of session settings
                            date.weekday().number_from_monday() as i32
                        }
                        Interval::DayOfYear => date.ordinal() as i32 - 1, // 0-based: 0..=364/365
                        Interval::Week => {
                            // Use session settings for week calculation
                            calculate_week_of_year(
                                date,
                                self.week_start(),
                                self.week_of_year_policy(),
                            )?
                        }
                        Interval::WeekOfYear => {
                            // Use session settings for week of year calculation
                            calculate_week_of_year(
                                date,
                                self.week_start(),
                                self.week_of_year_policy(),
                            )?
                        }
                        Interval::WeekIso => {
                            // Always use ISO week regardless of session settings
                            date.iso_week().week() as i32
                        }
                        Interval::Month => date.month() as i32, // 1..=12
                        Interval::Quarter => ((date.month() - 1) / 3 + 1) as i32, // 1..=4
                        Interval::Hour => naive.hour() as i32,
                        Interval::Minute => naive.minute() as i32,
                        Interval::Second => naive.second() as i32,
                    };

                    res.append_value(value as i64);
                }
            }
        }

        let res = res.finish();
        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

/// Register all date part extract functions with session parameters
pub fn register_udfs(
    registry: &mut dyn FunctionRegistry,
    session_params: &Arc<SessionParams>,
) -> DFResult<()> {
    for interval in Interval::iter() {
        registry.register_udf(Arc::new(ScalarUDF::from(DatePartExtractFunc::new(
            interval,
            session_params.clone(),
        ))))?;
    }
    Ok(())
}

/// Helper functions for week calculations
///
/// Convert `week_start` parameter to Weekday
/// `WEEK_START`:
/// 0: Legacy Snowflake behavior (ISO-like semantics, Monday start)
/// 1 (Monday) to 7 (Sunday): Week starts on the specified day
#[allow(clippy::match_same_arms)]
const fn week_start_to_weekday(week_start: usize) -> Weekday {
    match week_start {
        1 => Weekday::Mon,
        2 => Weekday::Tue,
        3 => Weekday::Wed,
        4 => Weekday::Thu,
        5 => Weekday::Fri,
        6 => Weekday::Sat,
        7 => Weekday::Sun,
        _ => Weekday::Mon, // Legacy Snowflake behavior (ISO-like)
    }
}

/// Calculate day of week based on `week_start` parameter
/// Returns 0-6 where 0 is the `week_start` day
/// For `DAYOFWEEK` function, this should return 0-6 based on the week start
#[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
fn calculate_day_of_week(date: NaiveDate, week_start: usize) -> i32 {
    let start_weekday = week_start_to_weekday(week_start);
    let current_weekday = date.weekday();

    let dow = (current_weekday.num_days_from_monday() as i32
        - start_weekday.num_days_from_monday() as i32
        + 7)
        % 7;
    if week_start == 0 && dow == 6 {
        0 // 0 means legacy Snowflake behavior (ISO-like semantics)
    } else {
        dow + 1 // 1-based for other days
    }
}

/// Calculate week number based on `week_of_year_policy`
/// `WEEK_OF_YEAR_POLICY`:
/// 0: ISO semantics - week belongs to a year if at least 4 days of that week are in that year
/// 1: January 1 is included in the first week, December 31 is included in the last week
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::as_conversions,
    clippy::cast_lossless
)]
fn calculate_week_of_year(
    date: NaiveDate,
    week_start: usize,
    week_of_year_policy: usize,
) -> DFResult<i32> {
    let res = match week_of_year_policy {
        0 => {
            // ISO semantics: week belongs to a year if at least 4 days are in that year
            let start_weekday = week_start_to_weekday(week_start);

            if week_start == 0 || week_start == 1 {
                // Use built-in ISO week for Monday start
                date.iso_week().week() as i32
            } else {
                // Calculate ISO-like week for other week starts
                let jan1 = NaiveDate::from_ymd_opt(date.year(), 1, 1).context(CantCastToSnafu {
                    v: "native_datetime",
                })?;

                // Find the first occurrence of the week start day in the year
                let mut first_week_start = jan1;
                while first_week_start.weekday() != start_weekday {
                    first_week_start = first_week_start.succ_opt().context(CantCastToSnafu {
                        v: "native_datetime",
                    })?;
                }

                // If January 1st is more than 3 days before the first week start,
                // then the first week belongs to the previous year (ISO rule)
                let days_before_first_week = (first_week_start - jan1).num_days();
                if days_before_first_week > 3 {
                    first_week_start += chrono::Duration::weeks(1);
                }

                if date < first_week_start {
                    // This date is in the last week of the previous year
                    let prev_dec31 = NaiveDate::from_ymd_opt(date.year() - 1, 12, 31).context(
                        CantCastToSnafu {
                            v: "native_datetime",
                        },
                    )?;

                    calculate_week_of_year(prev_dec31, week_start, week_of_year_policy)?
                } else {
                    let weeks_since_start = (date - first_week_start).num_weeks();
                    weeks_since_start as i32 + 1
                }
            }
        }
        1 => {
            // January 1 is in first week, December 31 is in last week
            let jan1 = NaiveDate::from_ymd_opt(date.year(), 1, 1).context(CantCastToSnafu {
                v: "native_datetime",
            })?;
            let start_weekday = week_start_to_weekday(week_start);

            // Find the first occurrence of the week start day on or after January 1
            let mut first_week_start = jan1;
            while first_week_start.weekday() != start_weekday
                && first_week_start > jan1 - chrono::Duration::days(7)
            {
                first_week_start = first_week_start.pred_opt().context(CantCastToSnafu {
                    v: "native_datetime",
                })?;
            }

            // If January 1 is not on the week start day, find the previous week start
            if jan1.weekday() == start_weekday {
                first_week_start = jan1;
            } else {
                let days_to_subtract = (jan1.weekday().num_days_from_monday() as i32
                    - start_weekday.num_days_from_monday() as i32
                    + 7)
                    % 7;
                first_week_start = jan1 - chrono::Duration::days(days_to_subtract as i64);
            }

            let weeks_since_start = (date - first_week_start).num_weeks();
            weeks_since_start as i32 + 1
        }
        _ => {
            // Default to ISO week
            date.iso_week().week() as i32
        }
    };
    Ok(res)
}

/// Calculate year of week based on `week_of_year_policy`
fn calculate_year_of_week(
    date: NaiveDate,
    week_start: usize,
    week_of_year_policy: usize,
) -> DFResult<i32> {
    let res = match week_of_year_policy {
        0 => {
            // ISO semantics: return the year that contains the majority of the week
            let start_weekday = week_start_to_weekday(week_start);

            if week_start == 0 || week_start == 1 {
                // Use built-in ISO year for Monday start
                date.iso_week().year()
            } else {
                // Calculate ISO-like year for other week starts
                let jan1 = NaiveDate::from_ymd_opt(date.year(), 1, 1).context(CantCastToSnafu {
                    v: "native_datetime",
                })?;

                // Find the first occurrence of the week start day in the year
                let mut first_week_start = jan1;
                while first_week_start.weekday() != start_weekday {
                    first_week_start = first_week_start.succ_opt().context(CantCastToSnafu {
                        v: "native_datetime",
                    })?;
                }

                // If January 1st is more than 3 days before the first week start,
                // then the first week belongs to the previous year (ISO rule)
                let days_before_first_week = (first_week_start - jan1).num_days();
                if days_before_first_week > 3 {
                    first_week_start += chrono::Duration::weeks(1);
                }

                if date < first_week_start {
                    // This date is in the last week of the previous year
                    date.year() - 1
                } else {
                    date.year()
                }
            }
        }
        1 => {
            // January 1 is in first week: always return the calendar year
            date.year()
        }
        _ => {
            // Default to ISO year
            date.iso_week().year()
        }
    };
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_common::config::ExtensionOptions;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let mut session_params = SessionParams::default();
        session_params.set("week_start", "1")?;
        let mut ctx = SessionContext::new();
        register_udfs(&mut ctx, &Arc::new(session_params))?;

        let sql = r#"SELECT '2025-04-11T23:39:20.123-07:00'::TIMESTAMP AS tstamp,
       YEAR('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "YEAR",
       QUARTER('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "QUARTER OF YEAR",
       MONTH('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "MONTH",
       DAY('2025-04-11T23:39:20.123'::TIMESTAMP) AS "DAY",
       DAYOFMONTH('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF MONTH",
       DAYOFYEAR('2025-04-11T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF YEAR";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------+------+-----------------+-------+-----+--------------+-------------+",
                "| tstamp                  | YEAR | QUARTER OF YEAR | MONTH | DAY | DAY OF MONTH | DAY OF YEAR |",
                "+-------------------------+------+-----------------+-------+-----+--------------+-------------+",
                "| 2025-04-12T06:39:20.123 | 2025 | 2               | 4     | 11  | 12           | 101         |",
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

        assert_batches_eq!(
            &[
                "+-------------------------+------+----------+--------------+--------------+------------------+",
                "| tstamp                  | WEEK | WEEK ISO | WEEK OF YEAR | YEAR OF WEEK | YEAR OF WEEK ISO |",
                "+-------------------------+------+----------+--------------+--------------+------------------+",
                "| 2016-01-03T06:39:20.123 | 53   | 53       | 53           | 2015         | 2015             |",
                "+-------------------------+------+----------+--------------+--------------+------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_week_policy() -> DFResult<()> {
        let mut session_params = SessionParams::default();
        session_params.set("week_start", "1")?;

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::Week,
            Arc::new(session_params),
        )));

        let sql = "SELECT WEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------------------+",
                "| week(Utf8(\"2016-01-02T23:39:20.123-07:00\")) |",
                "+---------------------------------------------+",
                "| 53                                          |",
                "+---------------------------------------------+",
            ],
            &result
        );

        let mut session_params = SessionParams::default();
        session_params.set("week_start", "1")?;
        session_params.set("week_of_year_policy", "1")?;
        ctx.register_udf(ScalarUDF::from(DatePartExtractFunc::new(
            Interval::Week,
            Arc::new(session_params),
        )));

        let sql = "SELECT WEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------------------+",
                "| week(Utf8(\"2016-01-02T23:39:20.123-07:00\")) |",
                "+---------------------------------------------+",
                "| 1                                           |",
                "+---------------------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[test]
    fn test_week_start_conversion() {
        // Test week_start_to_weekday function
        assert_eq!(week_start_to_weekday(0), Weekday::Mon); // Legacy
        assert_eq!(week_start_to_weekday(1), Weekday::Mon);
        assert_eq!(week_start_to_weekday(2), Weekday::Tue);
        assert_eq!(week_start_to_weekday(3), Weekday::Wed);
        assert_eq!(week_start_to_weekday(4), Weekday::Thu);
        assert_eq!(week_start_to_weekday(5), Weekday::Fri);
        assert_eq!(week_start_to_weekday(6), Weekday::Sat);
        assert_eq!(week_start_to_weekday(7), Weekday::Sun);
    }

    #[test]
    fn test_day_of_week_calculation() {
        // Test with a known date: 2024-01-01 is a Monday
        let monday = NaiveDate::from_ymd_opt(2024, 1, 1).expect("date");

        // With Monday start (week_start = 1), Monday should be day 0
        assert_eq!(calculate_day_of_week(monday, 1), 1);

        // With Sunday start (week_start = 7), Monday should be day 1
        assert_eq!(calculate_day_of_week(monday, 7), 2);

        // Test Tuesday (2024-01-02)
        let tuesday = NaiveDate::from_ymd_opt(2024, 1, 2).expect("date");
        assert_eq!(calculate_day_of_week(tuesday, 1), 2); // Monday start
        assert_eq!(calculate_day_of_week(tuesday, 7), 3); // Sunday start

        // Test Sunday (2024-01-07)
        let sunday = NaiveDate::from_ymd_opt(2024, 1, 7).expect("date");
        assert_eq!(calculate_day_of_week(sunday, 1), 7); // Monday start
        assert_eq!(calculate_day_of_week(sunday, 7), 1); // Sunday start
    }
}
