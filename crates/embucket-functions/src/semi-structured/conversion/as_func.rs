use crate::macros::make_udf_function;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

/// `AS_ARRAY` / `AS_OBJECT` SQL function
///
/// Converts a VARIANT value to an ARRAY or OBJECT type.
///
/// Syntax: `AS_ARRAY(<variant_expr>)`
///
/// Arguments:
/// - `variant_expr`: An expression that evaluates to a value of type VARIANT.
///
/// Example: `SELECT AS_ARRAY('[1,2,3]') as v;`
///
/// Returns:
/// - If the value of the `variant_expr` argument is of type ARRAY, the function returns a value of type ARRAY.
/// - If the type of `variant_expr` does not match the expected output type, the function returns NULL.
/// - If `variant_expr` is NULL, the function returns NULL as well.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for AsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["as_object".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "as_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let res = match arr.data_type() {
            DataType::Utf8 => arr,
            _ => Arc::new(StringArray::new_null(arr.len())),
        };

        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(AsFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(AsFunc::new()));
        let q = "SELECT AS_ARRAY('[1,2,3]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+",
                "| v       |",
                "+---------+",
                "| [1,2,3] |",
                "+---------+",
            ],
            &result
        );

        let q = "SELECT AS_ARRAY(NULL) as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        let q = "SELECT AS_ARRAY(123) as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        Ok(())
    }
}
