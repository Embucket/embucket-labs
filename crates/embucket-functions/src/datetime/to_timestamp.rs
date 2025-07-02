use crate::datetime::next_day::NextDayFunc;
use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Timelike, Utc, Weekday};
use datafusion::arrow::array::{
    Array, Date64Builder, Decimal128Array, Int32Array, Int64Array, StringArray, StringViewArray,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::{Coercible, Exact};
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::arrow::array::{
    ArrayRef, TimestampMicrosecondBuilder, TimestampSecondBuilder,
};
use datafusion_common::types::logical_string;
use datafusion_common::{ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

macro_rules! build_from_int_scale {
    ($args:expr,$arr:expr, $type:ty) => {{
        let scale = if $args.len() == 1 {
            0
        } else {
            if let ColumnarValue::Scalar(v) = &$args[1] {
                let scale = v.cast_to(&DataType::Int64)?;
                let ScalarValue::Int64(Some(v)) = &scale else {
                    return exec_err!("Second argument must integer");
                };

                *v
            } else {
                0
            }
        };

        let arr = $arr.as_any().downcast_ref::<$type>().unwrap();
        let arr: ArrayRef = match scale {
            0 => {
                let mut b = TimestampSecondBuilder::with_capacity(arr.len());
                for v in arr {
                    match v {
                        None => b.append_null(),
                        Some(v) => b.append_value(v as i64),
                    }
                }
                Arc::new(b.finish())
            }
            3 => {
                let mut b = TimestampMillisecondBuilder::with_capacity(arr.len());
                for v in arr {
                    match v {
                        None => b.append_null(),
                        Some(v) => b.append_value(v as i64),
                    }
                }
                Arc::new(b.finish())
            }
            6 => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(arr.len());
                for v in arr {
                    match v {
                        None => b.append_null(),
                        Some(v) => b.append_value(v as i64),
                    }
                }
                Arc::new(b.finish())
            }
            9 => {
                let mut b = TimestampNanosecondBuilder::with_capacity(arr.len());
                for v in arr {
                    match v {
                        None => b.append_null(),
                        Some(v) => b.append_value(v as i64),
                    }
                }
                Arc::new(b.finish())
            }
            _ => return exec_err!("Invalid scale"),
        };

        arr
    }};
}

macro_rules! build_from_int_string {
    ($args:expr,$arr:expr, $type:ty) => {{
        let format = if $args.len() == 1 {
            convert_snowflake_format_to_chrono("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM")
        } else {
            if let ColumnarValue::Scalar(v) = &$args[1] {
                let format = v.cast_to(&DataType::Utf8)?;
                let ScalarValue::Utf8(Some(v)) = &format else {
                    return exec_err!("Second argument must integer");
                };

                convert_snowflake_format_to_chrono(v)
            } else {
                convert_snowflake_format_to_chrono("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM")
            }
        };

        let arr = $arr.as_any().downcast_ref::<$type>().unwrap();
        let mut b = TimestampNanosecondBuilder::with_capacity(arr.len());
        for v in arr {
            match v {
                None => b.append_null(),
                Some(s) => {
                    if contains_only_digits(s) {
                        let i = s.parse::<i64>().unwrap(); // todo handle err
                        let scale = determine_timestamp_scale(i);
                        if scale == 0 {
                            b.append_value(i * 1000_000_000);
                        } else if scale == 3 {
                            b.append_value(i * 1000_000);
                        } else if scale == 6 {
                            b.append_value(i * 1000);
                        } else if scale == 9 {
                            b.append_value(i);
                        }
                    } else {
                        let v = NaiveDateTime::parse_from_str(s, &format).unwrap();
                        let t = v.timestamp_nanos_opt().unwrap();
                        b.append_value(t);
                    }
                }
            }
        }

        Arc::new(b.finish()) as ArrayRef
    }};
}

#[derive(Debug)]
pub struct ToTimestampFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ToTimestampFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToTimestampFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["to_timestamp_ntz".to_string()],
        }
    }
}

impl ScalarUDFImpl for ToTimestampFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        internal_err!("return_type_from_args should be called")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        if args.scalar_arguments.len() == 2 {
            if args.arg_types[0].is_numeric() {
                if let Some(v) = args.scalar_arguments[1] {
                    let scale = v.cast_to(&DataType::Int64)?;
                    let ScalarValue::Int64(Some(s)) = &scale else {
                        return exec_err!("Second argument must integer");
                    };
                    let s = *s;
                    return if s == 0 {
                        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Second,
                            None,
                        )))
                    } else if s == 3 {
                        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Millisecond,
                            None,
                        )))
                    } else if s == 6 {
                        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Microsecond,
                            None,
                        )))
                    } else if s == 9 {
                        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
                            TimeUnit::Nanosecond,
                            None,
                        )))
                    } else {
                        return plan_err!("invalid scale");
                    };
                }
            }
        }

        Ok(ReturnInfo::new_nullable(DataType::Timestamp(
            TimeUnit::Nanosecond,
            None,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        Ok(match arr.data_type() {
            DataType::Int64 => {
                let arr = build_from_int_scale!(args, arr, Int64Array);

                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            DataType::UInt64 => {
                let arr = build_from_int_scale!(args, arr, UInt64Array);

                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            DataType::Int32 => {
                let arr = build_from_int_scale!(args, arr, Int32Array);

                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            DataType::UInt32 => {
                let arr = build_from_int_scale!(args, arr, UInt32Array);

                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            DataType::Decimal128(_, s) => {
                let s = (*s as i128).pow(10);
                let scale = if args.len() == 1 {
                    0
                } else {
                    if let ColumnarValue::Scalar(v) = &args[1] {
                        let scale = v.cast_to(&DataType::Int64)?;
                        let ScalarValue::Int64(Some(v)) = &scale else {
                            return exec_err!("Second argument must integer");
                        };

                        *v
                    } else {
                        0
                    }
                };

                let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let arr: ArrayRef = match scale {
                    0 => {
                        let mut b = TimestampSecondBuilder::with_capacity(arr.len());
                        for v in arr {
                            match v {
                                None => b.append_null(),
                                Some(v) => b.append_value((v / s) as i64),
                            }
                        }
                        Arc::new(b.finish())
                    }
                    3 => {
                        let mut b = TimestampMillisecondBuilder::with_capacity(arr.len());
                        for v in arr {
                            match v {
                                None => b.append_null(),
                                Some(v) => b.append_value((v / s) as i64),
                            }
                        }
                        Arc::new(b.finish())
                    }
                    6 => {
                        let mut b = TimestampMicrosecondBuilder::with_capacity(arr.len());
                        for v in arr {
                            match v {
                                None => b.append_null(),
                                Some(v) => b.append_value((v / s) as i64),
                            }
                        }
                        Arc::new(b.finish())
                    }
                    9 => {
                        let mut b = TimestampNanosecondBuilder::with_capacity(arr.len());
                        for v in arr {
                            match v {
                                None => b.append_null(),
                                Some(v) => b.append_value((v / s) as i64),
                            }
                        }
                        Arc::new(b.finish())
                    }
                    _ => return exec_err!("Invalid scale"),
                };
                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            DataType::Utf8 => {
                let arr = build_from_int_string!(args, arr, StringArray);

                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            DataType::Utf8View => {
                let arr = build_from_int_string!(args, arr, StringViewArray);

                if arr.len() == 1 {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
                } else {
                    ColumnarValue::Array(Arc::new(arr))
                }
            }
            _ => panic!(),
        })
    }
}

fn contains_only_digits(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_digit())
}

pub fn determine_timestamp_scale(value: i64) -> u8 {
    const MILLIS_PER_YEAR: i64 = 31_536_000_000;
    const MICROS_PER_YEAR: i64 = 31_536_000_000_000;
    const NANOS_PER_YEAR: i64 = 31_536_000_000_000_000;

    let abs_value = value.abs();

    if abs_value < MILLIS_PER_YEAR {
        0
    } else if abs_value < MICROS_PER_YEAR {
        3
    } else if abs_value < NANOS_PER_YEAR {
        6
    } else {
        9
    }
}

pub fn convert_snowflake_format_to_chrono(snowflake_format: &str) -> String {
    let mut chrono_format = snowflake_format.to_string().to_lowercase();

    chrono_format = chrono_format.replace("yyyy", "%Y");
    chrono_format = chrono_format.replace("yy", "%y");

    chrono_format = chrono_format.replace("mm", "%m");
    chrono_format = chrono_format.replace("mon", "%b");
    chrono_format = chrono_format.replace("month", "%B");

    chrono_format = chrono_format.replace("dd", "%d");
    chrono_format = chrono_format.replace("dy", "%a");
    chrono_format = chrono_format.replace("day", "%A");

    chrono_format = chrono_format.replace("hh24", "%H");
    chrono_format = chrono_format.replace("hh", "%I");
    chrono_format = chrono_format.replace("am", "%P");
    chrono_format = chrono_format.replace("pm", "%P");

    chrono_format = chrono_format.replace("mi", "%M");

    chrono_format = chrono_format.replace("ss", "%S");

    chrono_format = chrono_format.replace("ff9", "%.9f");
    chrono_format = chrono_format.replace("ff6", "%.6f");
    chrono_format = chrono_format.replace("ff3", "%.3f");
    chrono_format = chrono_format.replace("ff", "%.f");

    chrono_format = chrono_format.replace("tzh:tzm", "%z");
    chrono_format = chrono_format.replace("tzhtzm", "%Z");

    chrono_format
}
crate::macros::make_udf_function!(NextDayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_scale() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 1970-01-12T13:46:40   | 1970-01-01T00:16:40   | 1970-01-01T00:00:01  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scaled() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_decimal() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 1970-01-12T13:46:40   | 1970-01-01T00:16:40   | 1970-01-01T00:00:01  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_decimal_scaled() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scale_int_str() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP('1000000000') AS "Scale in seconds",
       TO_TIMESTAMP('1000000000000') AS "Scale in milliseconds",
       TO_TIMESTAMP('1000000000000000') AS "Scale in microseconds",
       TO_TIMESTAMP('1000000000000000000') AS "Scale in nanoseconds";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_str_auto_format() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP('2024-04-05 01:02:03') as "a",
       TO_TIMESTAMP('2020-09-08T13:42:29.190855-05:00') AS "b",
       TO_TIMESTAMP('2020-09-08T13:42:29.190855Z') AS "c",
       TO_TIMESTAMP('2019-01-31 01:02:03.004') AS "d",
       TO_TIMESTAMP('2020-09-08T13:42:29Z') AS "e";"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| Scale in seconds    | Scale in milliseconds | Scale in microseconds | Scale in nanoseconds |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
                "| 2001-09-09T01:46:40 | 2001-09-09T01:46:40   | 2001-09-09T01:46:40   | 2001-09-09T01:46:40  |",
                "+---------------------+-----------------------+-----------------------+----------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_str_format() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToTimestampFunc::new()));

        let sql = r#"SELECT
       TO_TIMESTAMP('04/05/2024 01:02:03', 'mm/dd/yyyy hh24:mi:ss') as "a""#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| a                   |",
                "+---------------------+",
                "| 2024-04-05T01:02:03 |",
                "+---------------------+",
            ],
            &result
        );

        Ok(())
    }
}
