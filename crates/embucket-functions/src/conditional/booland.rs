use crate::array_to_boolean;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

// booland SQL function
// Computes the Boolean AND of two numeric expressions. In accordance with Boolean semantics:
// Syntax: BOOLAND( <expr1> , <expr2> )
// - <expr1>: Non-zero values (including negative numbers) are regarded as True. Zero values are regarded as False.
// - <expr2>: Non-zero values (including negative numbers) are regarded as True. Zero values are regarded as False.
// Note: `booland` returns
// - True if both expressions are non-zero.
// - False if both expressions are zero or one expression is zero and the other expression is non-zero or NULL.
// - NULL if both expressions are NULL or one expression is NULL and the other expression is non-zero.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BoolAndFunc {
    signature: Signature,
}

impl Default for BoolAndFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolAndFunc {
    #[must_use]
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
            ColumnarValue::Scalar(v) => array_to_boolean(&v.to_array()?),
            ColumnarValue::Array(arr) => array_to_boolean(arr),
        }?;
        let rhs = match &args.args[1] {
            ColumnarValue::Scalar(v) => array_to_boolean(&v.to_array()?),
            ColumnarValue::Array(arr) => array_to_boolean(arr),
        }?;

        let mut b = BooleanBuilder::with_capacity(lhs.len());

        for (lhs, rhs) in lhs.iter().zip(rhs.iter()) {
            if lhs.is_none() && rhs.is_none() {
                b.append_null();
                continue;
            }

            if (lhs.is_none() || rhs.is_none()) && (is_true(lhs) || is_true(rhs)) {
                b.append_null();
                continue;
            }

            b.append_value(is_true(lhs) && is_true(rhs));
        }

        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

#[must_use]
pub fn is_true(v: Option<bool>) -> bool {
    v.unwrap_or(false)
}

crate::macros::make_udf_function!(BoolAndFunc);

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
