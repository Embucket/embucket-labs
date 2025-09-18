use datafusion::arrow::array::{BooleanArray, BooleanBuilder};
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::exec_err;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

// equal_null SQL function
// Checks whether two expressions are equal. This function is NULL-safe, meaning it treats NULL
// values as known and comparable. This behavior differs from the standard equality operator (=),
// which treats NULLs as unknown and therefore not equal.
// Note: `equal_null` returns
// The return value depends on whether any of the inputs are NULL:
//
// Returns TRUE:
// EQUAL_NULL(<null>, <null>)
//
// Returns FALSE:
// EQUAL_NULL(<null>, <not_null>)
// EQUAL_NULL(<not_null>, <null>)
//
// In all other cases:
// EQUAL_NULL(<expr1>, <expr2>) behaves the same as <expr1> = <expr2>.

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EqualNullFunc {
    signature: Signature,
}

impl Default for EqualNullFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl EqualNullFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EqualNullFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "equal_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let lhs = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let rhs = match args[1].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        if lhs.len() != rhs.len() {
            return exec_err!("the first and second arguments must be of the same length");
        }

        let cmp_res = match apply_cmp(&args[0], &args[1], eq)? {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let cmp_res = cmp_res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let mut res = BooleanBuilder::with_capacity(cmp_res.len());
        for (i, v) in cmp_res.iter().enumerate() {
            if let Some(v) = v {
                res.append_value(v);
            } else if lhs.is_null(i) && rhs.is_null(i) {
                res.append_value(true);
            } else {
                res.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(res.finish())))
    }
}

crate::macros::make_udf_function!(EqualNullFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(EqualNullFunc::new()));

        let create = "CREATE OR REPLACE TABLE x (i integer);";
        ctx.sql(create).await?.collect().await?;

        let insert = "INSERT INTO x values (1), (2), (null);";
        ctx.sql(insert).await?.collect().await?;

        let q = "SELECT x1.i x1_i, x2.i x2_i FROM x x1, x x2  WHERE EQUAL_NULL(x1.i,x2.i);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------+------+",
                "| x1_i | x2_i |",
                "+------+------+",
                "| 1    | 1    |",
                "| 2    | 2    |",
                "|      |      |",
                "+------+------+",
            ],
            &result
        );
        Ok(())
    }
}
