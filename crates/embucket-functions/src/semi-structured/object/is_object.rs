use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::array::{Array, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

// is_array SQL function
// Returns TRUE if the VARIANT argument holds a value of type OBJECT.
// Syntax: IS_OBJECT( <variant_expr> )
// Arguments:
// - variant_expr
// An expression that evaluates to a value of type VARIANT.
// Example SELECT IS_OBJECT('{"a":1}') as v;
// Note `is_object` returns
// Returns a BOOLEAN value or NULL.
//
// - Returns TRUE if the VARIANT value contains an OBJECT value. Otherwise, returns FALSE.
// - If the input is NULL, returns NULL without reporting an error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsObjectFunc {
    signature: Signature,
}

impl Default for IsObjectFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsObjectFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsObjectFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "is_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut b = BooleanBuilder::with_capacity(arr.len());
        let input = as_string_array(&arr);
        for v in input {
            if let Some(v) = v {
                match serde_json::from_str::<Value>(v) {
                    Ok(v) => {
                        b.append_value(v.is_object());
                    }
                    Err(_) => b.append_value(false),
                }
            } else {
                b.append_null();
            }
        }

        let res = b.finish();
        Ok(if arr.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

crate::macros::make_udf_function!(IsObjectFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IsObjectFunc::new()));
        let q = "SELECT IS_OBJECT(NULL) as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        let q = "SELECT IS_OBJECT('invalid') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| v     |",
                "+-------+",
                "| false |",
                "+-------+",
            ],
            &result
        );

        let q = r#"SELECT IS_OBJECT('{"a":1}') as v;"#;
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+------+", "| v    |", "+------+", "| true |", "+------+",],
            &result
        );

        let q = "SELECT IS_OBJECT('[1,2,3]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| v     |",
                "+-------+",
                "| false |",
                "+-------+",
            ],
            &result
        );
        Ok(())
    }
}
