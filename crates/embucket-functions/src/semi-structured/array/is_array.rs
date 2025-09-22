use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::array::{Array, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

// is_array SQL function
// Returns TRUE if the VARIANT argument holds an ARRAY value.
// Syntax: IS_ARRAY( <variant_expr> )
// Arguments:
// - variant_expr
// An expression that evaluates to a value of type VARIANT.
// Example SELECT IS_ARRAY('[1,2,3]') as v;;
// Note `is_array` returns
// Returns a BOOLEAN value or NULL.
//
// - Returns TRUE if the VARIANT value contains an ARRAY value. Otherwise, returns FALSE.
// - If the input is NULL, returns NULL without reporting an error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsArrayFunc {
    signature: Signature,
}

impl Default for IsArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsArrayFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "is_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let arr = args[0].clone().into_array(number_rows)?;

        let mut b = BooleanBuilder::with_capacity(arr.len());
        let input = as_string_array(&arr);
        for v in input {
            if let Some(v) = v {
                match serde_json::from_str::<Value>(v) {
                    Ok(v) => {
                        b.append_value(v.is_array());
                    }
                    Err(_) => b.append_value(false),
                }
            } else {
                b.append_null();
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

crate::macros::make_udf_function!(IsArrayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IsArrayFunc::new()));
        let q = "SELECT IS_ARRAY(NULL) as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        let q = "SELECT IS_ARRAY('invalid') as v;";
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

        let q = r#"SELECT IS_ARRAY('{"a":1}') as v;"#;
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

        let q = "SELECT IS_ARRAY('[1,2,3]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+------+", "| v    |", "+------+", "| true |", "+------+",],
            &result
        );
        Ok(())
    }
}
