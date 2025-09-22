use chrono::{NaiveTime, ParseError, Timelike};
use datafusion::arrow::array::builder::Time64NanosecondBuilder;
use datafusion::arrow::array::types::Time64NanosecondType;
use datafusion::arrow::array::{Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
};
use datafusion::arrow::compute::kernels::cast_utils::Parser;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;
use std::sync::Arc;

// to_time |time | try_to_time SQL function
// Converts an input expression into a time.
// Syntax:
// TO_TIME( <string_expr> [, <format> ] )
// TO_TIME( <timestamp_expr> )
// TO_TIME( '<integer>' )
// TO_TIME( <variant_expr> )
//
// Required:
// <string_expr>, <timestamp_expr>, <'integer'>, or <variant_expr> — an expression to be converted to a time value:
// - <string_expr>: A string representing a time value to be converted.
// - <timestamp_expr>: A timestamp from which only the time portion is extracted.
// - <'integer'>: A string containing an integer that represents a number of seconds, milliseconds, microseconds, or nanoseconds since the Unix epoch (January 1, 1970). See usage notes for unit detection.
//
// For integers, the function calculates the number of seconds since the Unix epoch, then performs a modulo operation using the number of seconds in a day (86,400):
// number_of_seconds % 86400
// This result represents the number of seconds past midnight.
// Example:
// Given the value '31536002789', the function interprets it as milliseconds and converts it to the timestamp 1971-01-01 00:00:02.789.
// It extracts 31536002 seconds since the epoch and calculates:
// 31536002 % 86400 = 2,
// resulting in the time 00:00:02.
//
// For all other types, a conversion error is raised.
//
// Optional:
//
// <format> — A format specifier for <string_expr>
// HH24 - Two digits for hour (00 through 23). You must not specify AM / PM.
// HH12 - Two digits for hour (01 through 12). You can specify AM / PM.
// AM , PM - Ante meridiem (AM) / post meridiem (PM). Use this only with HH12 (not with HH24).
// MI - Two digits for minute (00 through 59).
// SS - Two digits for second (00 through 59).
// FF[0-9] - Fractional seconds with precision 0 (seconds) to 9 (nanoseconds), e.g. FF, FF0, FF3, FF9. Specifying FF is equivalent to FF9 (nanoseconds).
// Example: TO_TIME('11.15.00', 'HH24.MI.SS')
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimeFunc {
    signature: Signature,
    aliases: Vec<String>,
    is_try: bool,
}

impl Default for ToTimeFunc {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ToTimeFunc {
    #[must_use]
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::String(1), // TO_TIME( <string_expr> ), TO_TIME( '<integer>' )
                    TypeSignature::String(2), // TO_TIME( <string_expr> , <format> )
                    TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]), // TO_TIME( <timestamp_expr> )
                    TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Microsecond, None)]), // TO_TIME( <timestamp_expr> )
                    TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Millisecond, None)]), // TO_TIME( <timestamp_expr> )
                    TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Second, None)]), // TO_TIME( <timestamp_expr> )
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["time".to_string()],
            is_try,
        }
    }
}

impl ScalarUDFImpl for ToTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.is_try {
            "try_to_time"
        } else {
            "to_time"
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Time64(TimeUnit::Nanosecond))
    }

    #[allow(clippy::too_many_lines, clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arr = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let format = if args.len() == 2 {
            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = &args[1] {
                Some(v.to_owned())
            } else {
                return exec_err!("function expects a string as the second argument");
            }
        } else {
            None
        };

        let mut b = Time64NanosecondBuilder::with_capacity(arr.len());
        match arr.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                for v in arr {
                    if let Some(s) = v {
                        if let Some(format) = &format {
                            // TO_TIME( <string_expr> , <format> )
                            match parse_time(s, format) {
                                Ok(v) => {
                                    let hours = i64::from(v.hour());
                                    let minutes = i64::from(v.minute());
                                    let seconds = i64::from(v.second());
                                    let nanos = i64::from(v.nanosecond());

                                    let ts = (hours * 3600 + minutes * 60 + seconds)
                                        * 1_000_000_000
                                        + nanos;

                                    b.append_value(ts);
                                }
                                Err(v) => {
                                    if self.is_try {
                                        b.append_null();
                                    } else {
                                        return exec_err!(
                                            "can't parse time string with format: {v}"
                                        );
                                    }
                                }
                            }
                        } else {
                            match s.parse::<i64>() {
                                // TO_TIME( '<integer>' )
                                Ok(i) => {
                                    match s.len() {
                                        8 => {
                                            // seconds
                                            b.append_value(calc_nanos_since_midnight(
                                                i * 1_000_000_000,
                                            ));
                                        }
                                        11 => {
                                            // milliseconds
                                            b.append_value(calc_nanos_since_midnight(
                                                i * 1_000_000,
                                            ));
                                        }
                                        14 => {
                                            // microseconds
                                            b.append_value(calc_nanos_since_midnight(i * 1_000));
                                        }
                                        17 => b.append_value(calc_nanos_since_midnight(i)), // nanoseconds
                                        _ => {
                                            if self.is_try {
                                                b.append_null();
                                            } else {
                                                return exec_err!(
                                                    "function expects a valid integer (seconds, milliseconds, microseconds or nanoseconds)"
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    // TO_TIME( <string_expr> )
                                    match Time64NanosecondType::parse(s) {
                                        None => {
                                            if self.is_try {
                                                b.append_null();
                                            } else {
                                                return exec_err!("can't parse time string");
                                            }
                                        }
                                        Some(v) => b.append_value(v),
                                    }
                                }
                            }
                        }
                    } else {
                        b.append_null();
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let arr = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                for v in arr {
                    if let Some(v) = v {
                        b.append_value(calc_nanos_since_midnight(v * 1_000_000_000));
                    } else {
                        b.append_null();
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                for v in arr {
                    if let Some(v) = v {
                        b.append_value(calc_nanos_since_midnight(v * 1_000_000));
                    } else {
                        b.append_null();
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                for v in arr {
                    if let Some(v) = v {
                        b.append_value(calc_nanos_since_midnight(v * 1_000));
                    } else {
                        b.append_null();
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                for v in arr {
                    if let Some(v) = v {
                        b.append_value(calc_nanos_since_midnight(v));
                    } else {
                        b.append_null();
                    }
                }
            }
            _ => return exec_err!("unknown data type"),
        }

        let r = b.finish();
        Ok(ColumnarValue::Array(Arc::new(r)))
    }
}

const fn calc_nanos_since_midnight(nanos_total: i64) -> i64 {
    const NANOS_IN_DAY: i64 = 86_400 * 1_000_000_000;

    if nanos_total >= 0 {
        nanos_total % NANOS_IN_DAY
    } else {
        (nanos_total % NANOS_IN_DAY + NANOS_IN_DAY) % NANOS_IN_DAY
    }
}

fn parse_time(value: &str, format: &str) -> Result<NaiveTime, ParseError> {
    const MAP: [(&str, &str); 11] = [
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("MI", "%M"),
        ("SS", "%S"),
        ("FF", "%.f"),
        ("FF0", "%.f"),
        ("FF3", "%.3f"),
        ("FF6", "%.6f"),
        ("FF9", "%.9f"),
        ("AM", "%p"),
        ("PM", "%p"),
    ];

    let mut chrono_format = format.to_string();
    for (sql, chrono_fmt) in MAP {
        chrono_format = chrono_format.replace(sql, chrono_fmt);
    }

    NaiveTime::parse_from_str(value, &chrono_format)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_int_string() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimeFunc::new(false)));
        let q = "SELECT TO_TIME('31536001') as a, TO_TIME('31536002400') as b, TO_TIME('31536003600000') as c, TO_TIME('31536004900000000') as d";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+--------------+--------------+--------------+",
                "| a        | b            | c            | d            |",
                "+----------+--------------+--------------+--------------+",
                "| 00:00:01 | 00:00:02.400 | 00:00:03.600 | 00:00:04.900 |",
                "+----------+--------------+--------------+--------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_string() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimeFunc::new(false)));
        let q = "SELECT TO_TIME('13:30:00') as a, TO_TIME('13:30:00.000') as b";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+----------+",
                "| a        | b        |",
                "+----------+----------+",
                "| 13:30:00 | 13:30:00 |",
                "+----------+----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_string_formatted() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimeFunc::new(false)));
        let q = "SELECT TO_TIME('11.15.00', 'HH24.MI.SS') as a";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+",
                "| a        |",
                "+----------+",
                "| 11:15:00 |",
                "+----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimeFunc::new(false)));
        let q = "SELECT TO_TIME(CAST(1746617933 AS TIMESTAMP)) as a, TO_TIME(CAST('1970-01-01 02:46:41' AS TIMESTAMP)) as b";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+----------+",
                "| a        | b        |",
                "+----------+----------+",
                "| 11:38:53 | 02:46:41 |",
                "+----------+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_try() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimeFunc::new(true)));
        let q = "SELECT TRY_TO_TIME('err') as a, TRY_TO_TIME('13:30:00','err') as b, TRY_TO_TIME('123') as c";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---+---+---+",
                "| a | b | c |",
                "+---+---+---+",
                "|   |   |   |",
                "+---+---+---+",
            ],
            &result
        );

        Ok(())
    }
}
