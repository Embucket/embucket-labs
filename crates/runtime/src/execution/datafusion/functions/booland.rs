use arrow::datatypes::i256;
use arrow_schema::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use half::f16;
use std::any::Any;

// booland SQL function
// Computes the Boolean AND of two numeric expressions. In accordance with Boolean semantics:
// Syntax: BOOLAND( <expr1> , <expr2> )
// - <expr1>: Non-zero values (including negative numbers) are regarded as True. Zero values are regarded as False.
// - <expr2>: Non-zero values (including negative numbers) are regarded as True. Zero values are regarded as False.
// Note: `booland` returns
// - True if both expressions are non-zero.
// - False if both expressions are zero or one expression is zero and the other expression is non-zero or NULL.
// - NULL if both expressions are NULL or one expression is NULL and the other expression is non-zero.
#[derive(Debug)]
pub struct BoolAndFunc {
    signature: Signature,
}

impl Default for BoolAndFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolAndFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolAndFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "booland"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let lhs = match &args.args[0] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        let rhs = match &args.args[1] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        if lhs.is_null() && rhs.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        if (lhs.is_null() || rhs.is_null()) && (is_true(&lhs)? || is_true(&rhs)?) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::from(
            is_true(&lhs)? && is_true(&rhs)?,
        )))
    }
}

pub fn is_true(v: &ScalarValue) -> DFResult<bool> {
    if v.is_null() {
        return Ok(false);
    }
    Ok(match v {
        ScalarValue::Float16(Some(v)) => *v != f16::from_f32(0.0),
        ScalarValue::Float32(Some(v)) => *v != 0.,
        ScalarValue::Float64(Some(v)) => *v != 0.,
        ScalarValue::Decimal128(Some(v), _, _) => *v != 0,
        ScalarValue::Decimal256(Some(v), _, _) => *v != i256::from_i128(0),
        ScalarValue::Int8(Some(v)) => *v != 0,
        ScalarValue::Int16(Some(v)) => *v != 0,
        ScalarValue::Int32(Some(v)) => *v != 0,
        ScalarValue::Int64(Some(v)) => *v != 0,
        ScalarValue::UInt8(Some(v)) => *v != 0,
        ScalarValue::UInt16(Some(v)) => *v != 0,
        ScalarValue::UInt32(Some(v)) => *v != 0,
        ScalarValue::UInt64(Some(v)) => *v != 0,
        _ => {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Unsupported type {:?}",
                v.data_type()
            )));
        }
    })
}

super::macros::make_udf_function!(BoolAndFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolAndFunc::new()));
        let q = "SELECT BOOLAND(1, -2), BOOLAND(0, 2.35), BOOLAND(0, 0), BOOLAND(0, NULL), BOOLAND(NULL, 3), BOOLAND(NULL, NULL);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
        &[
"+-----------------------------+---------------------------------+----------------------------+------------------------+------------------------+--------------------+",
"| booland(Int64(1),Int64(-2)) | booland(Int64(0),Float64(2.35)) | booland(Int64(0),Int64(0)) | booland(Int64(0),NULL) | booland(NULL,Int64(3)) | booland(NULL,NULL) |",
"+-----------------------------+---------------------------------+----------------------------+------------------------+------------------------+--------------------+",
"| true                        | false                           | false                      | false                  |                        |                    |",
"+-----------------------------+---------------------------------+----------------------------+------------------------+------------------------+--------------------+",
        ],
        &result
    );

        Ok(())
    }
}
