use crate::datetime_errors::Result;
use crate::datetime_errors::{
    CantGetNanosecondsSnafu, CantParseTimezoneSnafu, InvalidDatetimeSnafu, InvalidTimestampSnafu,
};
use crate::session_params::SessionParams;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{ArrayRef, TimestampNanosecondBuilder};
use datafusion::arrow::compute::cast_with_options;
use datafusion::arrow::datatypes::DataType::{Timestamp, Utf8};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::datatypes::{Field, FieldRef};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{ColumnarValue, Signature, TIMEZONE_WILDCARD, Volatility};
use datafusion_common::cast::as_timestamp_nanosecond_array;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use snafu::{OptionExt, ResultExt};
use std::any::Any;
use std::sync::Arc;

/// `CONVERT_TIMEZONE` SQL function
///
/// Converts a timestamp from one timezone to another.
///
/// Syntax: `CONVERT_TIMEZONE(<target_timezone>, <source_timezone>, <source_timestamp_ntz>)`
/// Syntax: `CONVERT_TIMEZONE(<target_timezone>, <source_timestamp_tz>)`
///
/// Arguments:
/// - `<target_timezone>`: A string representing the target timezone (e.g. `America/Los_Angeles`).
/// - `<source_timezone>`: A string representing the source timezone (e.g. `America/Los_Angeles`).
/// - `<source_timestamp_ntz>`: A timestamp without timezone to convert.
/// - `<source_timestamp_tz>`: A timestamp with timezone to convert.
///
/// Example: `SELECT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', '2024-01-01 14:00:00'::TIMESTAMP) AS conv;`
///
/// Returns:
/// - For the 3-argument version, returns a value of type `TIMESTAMP_NTZ`.
/// - For the 2-argument version, returns a value of type `TIMESTAMP_TZ`.
///
/// Usage notes:
/// For the 3-argument version, the “wallclock” time in the result represents the same moment in time
/// as the input “wallclock” in the input time zone, but in the target time zone.
///
/// For the 2-argument version, the `source_timestamp` argument typically includes the time zone.
/// If the value is of type `TIMESTAMP_TZ`, the time zone is taken from its value. Otherwise, the current session time zone is used.
///
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTimezoneFunc {
    signature: Signature,
    session_params: Arc<SessionParams>,
}

impl Default for ConvertTimezoneFunc {
    fn default() -> Self {
        Self::new(Arc::new(SessionParams::default()))
    }
}

impl ConvertTimezoneFunc {
    #[must_use]
    pub fn new(session_params: Arc<SessionParams>) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![
                        Utf8,
                        Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Utf8, Timestamp(TimeUnit::Second, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(TimeUnit::Millisecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(TimeUnit::Microsecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(TimeUnit::Nanosecond, None)]),
                ],
                Volatility::Immutable,
            ),
            session_params,
        }
    }

    #[must_use]
    pub fn timezone(&self) -> String {
        self.session_params
            .get_property("timezone")
            .unwrap_or_else(|| "America/Los_Angeles".to_string())
    }
}

impl ScalarUDFImpl for ConvertTimezoneFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "convert_timezone"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        internal_err!("return_type_from_args should be called")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        match args.arg_fields.len() {
            2 => {
                let tz = match &args.scalar_arguments[0] {
                    Some(
                        ScalarValue::Utf8(Some(part))
                        | ScalarValue::Utf8View(Some(part))
                        | ScalarValue::LargeUtf8(Some(part)),
                    ) => part.clone(),
                    _ => return internal_err!("Invalid target_tz type"),
                };

                match &args.arg_fields[1].data_type() {
                    Timestamp(_, _) => Ok(Arc::new(Field::new(
                        self.name(),
                        Timestamp(TimeUnit::Nanosecond, Some(Arc::from(tz.into_boxed_str()))),
                        false,
                    ))),
                    _ => internal_err!("Invalid source_timestamp_tz type"),
                }
            }
            3 => match &args.arg_fields[2].data_type() {
                Timestamp(_, _) => Ok(Arc::new(Field::new(
                    self.name(),
                    Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ))),
                _ => internal_err!("Invalid source_timestamp_ntz type"),
            },
            other => {
                internal_err!(
                    "This function can only take two or three arguments, got {}",
                    other
                )
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let (source_tz, target_tz, arr) = if args.len() == 2 {
            match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz))) => {
                    let arr = match &args[1] {
                        ColumnarValue::Array(arr) => arr.to_owned(),
                        ColumnarValue::Scalar(v) => v.to_array()?,
                    };
                    (None, Some(tz.clone()), arr)
                }
                _ => return internal_err!("Invalid source_tz type format"),
            }
        } else if args.len() == 3 {
            let source_tz = match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => Some(part.clone()),
                _ => return internal_err!("Invalid source_tz type format"),
            };

            let target_tz = match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => Some(part.clone()),
                _ => return internal_err!("Invalid target_tz type format"),
            };

            let arr = match &args[2] {
                ColumnarValue::Array(arr) => arr.to_owned(),
                ColumnarValue::Scalar(v) => v.to_array()?,
            };

            (source_tz, target_tz, arr)
        } else {
            return internal_err!("Invalid number of arguments");
        };

        match (source_tz, target_tz, arr) {
            (Some(source_tz), Some(target_tz), arr) => build_array(&arr, &source_tz, &target_tz),
            (None, Some(target_tz), arr) => match arr.data_type() {
                Timestamp(_, Some(_)) => {
                    let arr = cast_with_options(
                        &arr,
                        &Timestamp(
                            TimeUnit::Nanosecond,
                            Some(Arc::from(target_tz.into_boxed_str())),
                        ),
                        &DEFAULT_CAST_OPTIONS,
                    )?;

                    Ok(ColumnarValue::Array(arr))
                }
                Timestamp(_, None) => build_array(&arr, &self.timezone(), &target_tz),
                _ => internal_err!("Invalid source_timestamp type"),
            },
            _ => internal_err!("Invalid arguments"),
        }
    }
}

fn build_array(arr: &ArrayRef, source_tz: &str, target_tz: &str) -> DFResult<ColumnarValue> {
    let source_tz = source_tz.parse::<Tz>().context(CantParseTimezoneSnafu)?;
    let target_tz = target_tz.parse::<Tz>().context(CantParseTimezoneSnafu)?;

    let arr = as_timestamp_nanosecond_array(&arr)?;
    let mut b = TimestampNanosecondBuilder::with_capacity(arr.len());
    for v in arr {
        match v {
            Some(v) => {
                let v = convert_timezone(source_tz, target_tz, v)?;
                b.append_value(v);
            }
            None => b.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(b.finish())))
}
#[allow(clippy::cast_sign_loss, clippy::as_conversions)]
fn convert_timezone(
    source_timezone: Tz,
    target_timezone: Tz,
    source_timestamp_ntz: i64,
) -> Result<i64> {
    // Convert nanoseconds to seconds and nanoseconds remainder
    let secs = source_timestamp_ntz / 1_000_000_000;
    let nanos = (source_timestamp_ntz % 1_000_000_000) as u32;

    // Create a naive datetime from the timestamp (treating it as epoch time but interpreting as naive)
    let naive_dt = DateTime::from_timestamp(secs, nanos)
        .context(InvalidTimestampSnafu)?
        .naive_utc(); // Get the naive representation

    // Interpret this naive datetime as being in the source timezone
    let source_dt = source_timezone
        .from_local_datetime(&naive_dt)
        .single()
        .context(InvalidDatetimeSnafu)?;

    // Convert to target timezone
    let target_dt = source_dt.with_timezone(&target_timezone);

    // Get the naive local time in the target timezone
    let target_naive = target_dt.naive_local();

    // Convert back to UTC nanoseconds (treating the target naive time as if it were UTC)
    let target_utc = Utc
        .from_local_datetime(&target_naive)
        .single()
        .context(InvalidDatetimeSnafu)?;

    // Return as nanoseconds
    target_utc
        .timestamp_nanos_opt()
        .context(CantGetNanosecondsSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_ntz() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ConvertTimezoneFunc::default()));

        let sql = "SELECT CONVERT_TIMEZONE('America/Los_Angeles','America/New_York','2024-01-01 14:00:00'::TIMESTAMP) AS conv;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| conv                |",
                "+---------------------+",
                "| 2024-01-01T17:00:00 |",
                "+---------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_tz() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ConvertTimezoneFunc::default()));

        let sql = "SELECT CONVERT_TIMEZONE('America/Los_Angeles','2024-01-01 14:00:00 -07:00'::TIMESTAMP) AS conv;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------+",
                "| conv                      |",
                "+---------------------------+",
                "| 2024-01-01T13:00:00-08:00 |",
                "+---------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
