use crate::execution::datafusion::functions::booland::is_true;
use arrow_schema::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

// boolor SQL function
// Computes the Boolean OR of two numeric expressions. In accordance with Boolean semantics:
// - Non-zero values (including negative numbers) are regarded as True.
// - Zero values are regarded as False.
// Syntax: BOOLOR( <expr1> , <expr2> )
// Note: `boolor` returns
// - True if both expressions are non-zero or one expression is non-zero and the other expression is zero or NULL.
// - False if both expressions are zero.
// - NULL if both expressions are NULL or one expression is NULL and the other expression is zero.
#[derive(Debug)]
pub struct BoolOrFunc {
    signature: Signature,
}

impl Default for BoolOrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolOrFunc {
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

        if (lhs.is_null() || rhs.is_null()) && (!is_true(&lhs)? && !is_true(&rhs)?) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::from(
            is_true(&lhs)? || is_true(&rhs)?,
        )))
    }
}

super::macros::make_udf_function!(BoolOrFunc);

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
