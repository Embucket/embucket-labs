use crate::array_to_boolean;
use datafusion::arrow::array::{
    ArrayRef, Decimal256Array, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::arrow::array::Decimal128Array;
use datafusion_common::exec_err;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

macro_rules! build_array {
    ($arr:expr, $type:ty) => {{
        let bool_arr = array_to_boolean(&$arr)?;
        let arr = $arr.as_any().downcast_ref::<$type>().unwrap();
        let mut builder = <$type>::builder(arr.len());
        for (i, v) in arr.iter().enumerate() {
            if bool_arr.value(i) {
                builder.append_option(v);
            } else {
                builder.append_null();
            }
        }

        Arc::new(builder.finish())
    }};
}

// function nullifzero
// Returns NULL if the argument is equal to 0; otherwise, returns the argument itself.
// Syntax: NULLIFZERO( <expr> )
// Arguments
// - <expr>
// The input must be an expression that evaluates to a numeric value.
// If the input expression evaluates to 0, the function returns NULL. Otherwise, it returns the value of the input expression.
//
// The return type is NUMBER(p, s) for fixed-point inputs or DOUBLE for floating-point inputs.
//
// For fixed-point numbers, the precision (p) and scale (s) of the return type depend on the input expression.
// For example, if the input is 3.14159, the return type will be NUMBER(7, 5).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NullIfZeroFunc {
    signature: Signature,
}

impl Default for NullIfZeroFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NullIfZeroFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NullIfZeroFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "nullifzero"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    #[allow(clippy::cognitive_complexity)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let res: ArrayRef = match arr.data_type() {
            DataType::Int8 => build_array!(arr, Int8Array),
            DataType::Int16 => build_array!(arr, Int16Array),
            DataType::Int32 => build_array!(arr, Int32Array),
            DataType::Int64 => build_array!(arr, Int64Array),
            DataType::UInt8 => build_array!(arr, UInt8Array),
            DataType::UInt16 => build_array!(arr, UInt16Array),
            DataType::UInt32 => build_array!(arr, UInt32Array),
            DataType::UInt64 => build_array!(arr, UInt64Array),
            DataType::Float16 => build_array!(arr, Float16Array),
            DataType::Float32 => build_array!(arr, Float32Array),
            DataType::Float64 => build_array!(arr, Float64Array),
            DataType::Decimal128(_, _) => build_array!(arr, Decimal128Array),
            DataType::Decimal256(_, _) => build_array!(arr, Decimal256Array),
            _ => return exec_err!("function does not support type {}", arr.data_type()),
        };

        Ok(ColumnarValue::Array(res))
    }
}

crate::macros::make_udf_function!(NullIfZeroFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(NullIfZeroFunc::new()));
        let q = "SELECT NULLIFZERO(0);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------------------+",
                "| nullifzero(Int64(0)) |",
                "+----------------------+",
                "|                      |",
                "+----------------------+",
            ],
            &result
        );

        let q = "SELECT NULLIFZERO(52);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------------+",
                "| nullifzero(Int64(52)) |",
                "+-----------------------+",
                "| 52                    |",
                "+-----------------------+",
            ],
            &result
        );

        let q = "SELECT NULLIFZERO(3.14159);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------------------+",
                "| nullifzero(Float64(3.14159)) |",
                "+------------------------------+",
                "| 3.14159                      |",
                "+------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
