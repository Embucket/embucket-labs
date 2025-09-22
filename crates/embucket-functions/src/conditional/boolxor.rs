use crate::array_to_boolean;
use crate::conditional::booland::is_true;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

// boolxor SQL function
// Computes the Boolean XOR of two numeric expressions (i.e. one of the expressions, but not both expressions, is TRUE). In accordance with Boolean semantics:
// - Non-zero values (including negative numbers) are regarded as True.
// - Zero values are regarded as False.
// Syntax: BOOLXOR( <expr1> , <expr2> )
// Note: `boolxor` returns
// - True if one expression is non-zero and the other expression is zero.
// - False if both expressions are non-zero or both expressions are zero.
// - NULL if one or both expressions are NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BoolXorFunc {
    signature: Signature,
}

impl Default for BoolXorFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolXorFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolXorFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "boolxor"
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

            if lhs.is_none() || rhs.is_none() {
                b.append_null();
                continue;
            }

            if (is_true(lhs) && !is_true(rhs)) || (!is_true(lhs) && is_true(rhs)) {
                b.append_value(true);
            } else if (is_true(lhs) && is_true(rhs)) || (!is_true(lhs) && !is_true(rhs)) {
                b.append_value(false);
            } else {
                b.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

crate::macros::make_udf_function!(BoolXorFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolXorFunc::new()));
        let q = "SELECT BOOLXOR(2, 0), BOOLXOR(1, -1), BOOLXOR(0, 0), BOOLXOR(NULL, 3), BOOLXOR(NULL, 0), BOOLXOR(NULL, NULL);
";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------------------------+-----------------------------+----------------------------+------------------------+------------------------+--------------------+",
                "| boolxor(Int64(2),Int64(0)) | boolxor(Int64(1),Int64(-1)) | boolxor(Int64(0),Int64(0)) | boolxor(NULL,Int64(3)) | boolxor(NULL,Int64(0)) | boolxor(NULL,NULL) |",
                "+----------------------------+-----------------------------+----------------------------+------------------------+------------------------+--------------------+",
                "| true                       | false                       | false                      |                        |                        |                    |",
                "+----------------------------+-----------------------------+----------------------------+------------------------+------------------------+--------------------+",
            ],
            &result
        );
        Ok(())
    }
}
