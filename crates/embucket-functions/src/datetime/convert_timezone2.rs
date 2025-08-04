use arrow_schema::DataType::{Timestamp, Utf8};
use arrow_schema::{DataType, TimeUnit};
use datafusion::arrow::array::{TimestampNanosecondArray, TimestampNanosecondBuilder};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{ColumnarValue, Signature, TIMEZONE_WILDCARD, Volatility};
use datafusion_common::cast::as_timestamp_nanosecond_array;
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ConvertTimezoneFunc {
    signature: Signature,
}

impl Default for ConvertTimezoneFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ConvertTimezoneFunc {
    #[must_use]
    pub fn new() -> Self {
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
        }
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

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> DFResult<ReturnInfo> {
        match args.arg_types.len() {
            2 => {
                let tz = match &args.scalar_arguments[0] {
                    Some(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return internal_err!("Invalid target_tz type"),
                };

                match &args.arg_types[1] {
                    Timestamp(tu, _) => Ok(ReturnInfo::new_non_nullable(Timestamp(
                        *tu,
                        Some(Arc::from(tz.into_boxed_str())),
                    ))),
                    _ => internal_err!("Invalid source_timestamp_tz type"),
                }
            }
            3 => match &args.arg_types[2] {
                Timestamp(tu, _) => Ok(ReturnInfo::new_non_nullable(Timestamp(*tu, None))),
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
            (Some(source_tz), Some(target_tz), arr) => {
                let arr = cast_with_options(
                    &arr,
                    &Timestamp(
                        TimeUnit::Nanosecond,
                        Some(Arc::from(source_tz.into_boxed_str())),
                    ),
                    &CastOptions::default(),
                )?;

                let arr = cast_with_options(
                    &arr,
                    &Timestamp(
                        TimeUnit::Nanosecond,
                        Some(Arc::from(target_tz.clone().into_boxed_str())),
                    ),
                    &CastOptions::default(),
                )?;

                let arr = as_timestamp_nanosecond_array(&arr)?.to_owned().with_timezone("+00");
                let mut b = TimestampNanosecondBuilder::with_capacity(arr.len());
                for v in arr.iter() {
                    dbg!(&v);
                    b.append_option(v);
                }

                Ok(ColumnarValue::Array(Arc::new(b.finish())))
            }
            _ => internal_err!("Invalid arguments"),
        }
    }
}

crate::macros::make_udf_function!(ConvertTimezoneFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ConvertTimezoneFunc::new()));

        let sql = "SELECT CONVERT_TIMEZONE('America/Los_Angeles','America/New_York','2024-01-01 14:00:00'::TIMESTAMP) AS conv;";
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

        Ok(())
    }
}
