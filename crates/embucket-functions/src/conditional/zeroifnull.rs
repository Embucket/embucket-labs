use datafusion::arrow::array::{
    Decimal128Array, Decimal128Builder, Float32Array, Float32Builder, Float64Array, Float64Builder,
    Int8Array, Int8Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, UInt8Array, UInt8Builder, UInt16Array, UInt16Builder, UInt32Array, UInt32Builder,
    UInt64Array, UInt64Builder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

macro_rules! handle_numeric_type {
    ($arr:expr, $array_type:ty, $builder_type:ty, $zero_value:expr) => {{
        let arr = $arr.as_any().downcast_ref::<$array_type>().unwrap();
        let mut builder = <$builder_type>::with_capacity(arr.len());
        for v in arr {
            if v.is_some() {
                builder.append_option(v);
            } else {
                builder.append_value($zero_value);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }};
}

/// `ZEROIFNULL` SQL function
///
/// Returns 0 if the argument is NULL; otherwise, returns the argument itself.
///
/// Syntax: `ZEROIFNULL( <expr> )`
///
/// Arguments:
/// - `expr`: The input expression.
///
/// Example: `SELECT ZEROIFNULL(1) AS value;`
///
/// Returns:
/// - Returns 0 if the input expression is NULL.
/// - Returns the value of the input expression if it is not NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ZeroIfNullFunc {
    signature: Signature,
}

impl Default for ZeroIfNullFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ZeroIfNullFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ZeroIfNullFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "zeroifnull"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arr = match args.args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        match arr.data_type() {
            DataType::Float64 => {
                handle_numeric_type!(arr, Float64Array, Float64Builder, 0.0)
            }
            DataType::Int8 => {
                handle_numeric_type!(arr, Int8Array, Int8Builder, 0)
            }
            DataType::Int16 => {
                handle_numeric_type!(arr, Int16Array, Int16Builder, 0)
            }
            DataType::Int32 => {
                handle_numeric_type!(arr, Int32Array, Int32Builder, 0)
            }
            DataType::Int64 => {
                handle_numeric_type!(arr, Int64Array, Int64Builder, 0)
            }
            DataType::UInt8 => {
                handle_numeric_type!(arr, UInt8Array, UInt8Builder, 0)
            }
            DataType::UInt16 => {
                handle_numeric_type!(arr, UInt16Array, UInt16Builder, 0)
            }
            DataType::UInt32 => {
                handle_numeric_type!(arr, UInt32Array, UInt32Builder, 0)
            }
            DataType::UInt64 => {
                handle_numeric_type!(arr, UInt64Array, UInt64Builder, 0)
            }
            DataType::Float32 => {
                handle_numeric_type!(arr, Float32Array, Float32Builder, 0.0)
            }
            DataType::Decimal128(p, s) => {
                let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let mut builder =
                    Decimal128Builder::with_capacity(arr.len()).with_precision_and_scale(*p, *s)?;
                for v in arr {
                    if v.is_some() {
                        builder.append_option(v);
                    } else {
                        builder.append_value(0);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            _ => Err(datafusion_common::DataFusionError::Execution(
                "zeroifnull function only supports numeric types".to_string(),
            )),
        }
    }
}

crate::macros::make_udf_function!(ZeroIfNullFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_zeroifnull() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ZeroIfNullFunc::new()));

        let result = ctx
            .sql("SELECT zeroifnull(3.14),zeroifnull(1),zeroifnull(3.14::decimal(20,10)) as d")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+---------------------------+----------------------+--------------+",
                "| zeroifnull(Float64(3.14)) | zeroifnull(Int64(1)) | d            |",
                "+---------------------------+----------------------+--------------+",
                "| 3.14                      | 1                    | 3.1400000000 |",
                "+---------------------------+----------------------+--------------+",
            ],
            &result
        );

        let result = ctx.sql("SELECT zeroifnull(null),zeroifnull(0.0),zeroifnull(0),zeroifnull(0.0::decimal(20,10)) as d").await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------+------------------------+----------------------+--------------+",
                "| zeroifnull(NULL) | zeroifnull(Float64(0)) | zeroifnull(Int64(0)) | d            |",
                "+------------------+------------------------+----------------------+--------------+",
                "| 0.0              | 0.0                    | 0                    | 0.0000000000 |",
                "+------------------+------------------------+----------------------+--------------+",
            ],
            &result
        );

        Ok(())
    }
}
