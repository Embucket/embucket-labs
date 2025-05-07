use arrow::compute::cast;
use arrow::compute::kernels::cast_utils::Parser;
use arrow_array::builder::{Time64NanosecondBuilder, TimestampNanosecondBuilder};
use arrow_array::types::{Time64NanosecondType, TimestampNanosecondType};
use arrow_array::{Array, ArrayRef, StringArray, Time64NanosecondArray, TimestampNanosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{NaiveTime, ParseError, Timelike};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{exec_err, plan_err, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;
use std::collections::HashMap;
use std::num::ParseIntError;
use std::sync::Arc;

#[derive(Debug)]
pub struct ToTimeFunc {
    signature: Signature,
    aliases: Vec<String>,
    try_: bool,
}

impl Default for ToTimeFunc {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ToTimeFunc {
    pub fn new(try_: bool) -> Self {
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
            try_,
        }
    }
}

impl ScalarUDFImpl for ToTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.try_ {
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
                                    let hours = v.hour() as i64;
                                    let minutes = v.minute() as i64;
                                    let seconds = v.second() as i64;
                                    let nanos = v.nanosecond() as i64;

                                    let ts = (hours * 3600 + minutes * 60 + seconds)
                                        * 1_000_000_000
                                        + nanos;

                                    b.append_value(ts)
                                }
                                Err(v) => {
                                    if self.try_ {
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
                                            if self.try_ {
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
                                            if self.try_ {
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
            DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = cast(arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
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

fn calc_nanos_since_midnight(nanos_total: i64) -> i64 {
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
    use arrow::util::pretty::print_batches;
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
