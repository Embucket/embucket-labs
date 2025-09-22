use crate::array_to_boolean;
use crate::conditional::booland::is_true;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

// boolor SQL function
// Computes the Boolean OR of two numeric expressions. In accordance with Boolean semantics:
// - Non-zero values (including negative numbers) are regarded as True.
// - Zero values are regarded as False.
// Syntax: BOOLOR( <expr1> , <expr2> )
// Note: `boolor` returns
// - True if both expressions are non-zero or one expression is non-zero and the other expression is zero or NULL.
// - False if both expressions are zero.
// - NULL if both expressions are NULL or one expression is NULL and the other expression is zero.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BoolOrFunc {
    signature: Signature,
}

impl Default for BoolOrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolOrFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolOrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "boolor"
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
            if (lhs.is_none() || rhs.is_none()) && (!is_true(lhs) && !is_true(rhs)) {
                b.append_null();
                continue;
            }

            b.append_value(is_true(lhs) || is_true(rhs));
        }

        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

crate::macros::make_udf_function!(BoolOrFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolOrFunc::new()));
        let q = "SELECT BOOLOR(1, 2), BOOLOR(-1.35, 0), BOOLOR(3, NULL), BOOLOR(0, 0), BOOLOR(NULL, 0), BOOLOR(NULL, NULL);
";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------+---------------------------------+-----------------------+---------------------------+-----------------------+-------------------+",
                "| boolor(Int64(1),Int64(2)) | boolor(Float64(-1.35),Int64(0)) | boolor(Int64(3),NULL) | boolor(Int64(0),Int64(0)) | boolor(NULL,Int64(0)) | boolor(NULL,NULL) |",
                "+---------------------------+---------------------------------+-----------------------+---------------------------+-----------------------+-------------------+",
                "| true                      | true                            | true                  | false                     |                       |                   |",
                "+---------------------------+---------------------------------+-----------------------+---------------------------+-----------------------+-------------------+",
            ],
            &result
        );
        Ok(())
    }
}
