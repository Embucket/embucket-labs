use crate::execution::datafusion::functions::booland::is_true;
use arrow_schema::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

// boolxor SQL function
// Computes the Boolean XOR of two numeric expressions (i.e. one of the expressions, but not both expressions, is TRUE). In accordance with Boolean semantics:
// - Non-zero values (including negative numbers) are regarded as True.
// - Zero values are regarded as False.
// Syntax: BOOLXOR( <expr1> , <expr2> )
// Note: `boolxor` returns
// - True if one expression is non-zero and the other expression is zero.
// - False if both expressions are non-zero or both expressions are zero.
// - NULL if one or both expressions are NULL.
#[derive(Debug)]
pub struct BoolXorFunc {
    signature: Signature,
}

impl Default for BoolXorFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolXorFunc {
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

        if lhs.is_null() || rhs.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        if (is_true(&lhs)? && !is_true(&rhs)?) || (!is_true(&lhs)? && is_true(&rhs)?) {
            Ok(ColumnarValue::Scalar(ScalarValue::from(Some(true))))
        } else if (is_true(&lhs)? && is_true(&rhs)?) || (!is_true(&lhs)? && !is_true(&rhs)?) {
            Ok(ColumnarValue::Scalar(ScalarValue::from(Some(false))))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
        }
    }
}

super::macros::make_udf_function!(BoolXorFunc);

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
