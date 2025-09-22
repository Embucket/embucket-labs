use crate::json::{encode_array, encode_scalar};
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_slice};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPositionUDF {
    signature: Signature,
}

#[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
impl ArrayPositionUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn array_position(element: &Value, array: &Value) -> Option<i64> {
        if let Value::Array(arr) = array {
            // Find the position of the element in the array
            for (index, item) in arr.iter().enumerate() {
                if item == element {
                    return Some(index as i64);
                }
            }
        }
        None
    }
}

impl Default for ArrayPositionUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayPositionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let element = args
            .first()
            .ok_or_else(|| errors::ExpectedElementArgumentSnafu.build())?;
        let array = args
            .get(1)
            .ok_or_else(|| errors::ArrayArgumentExpectedSnafu.build())?;

        match (element, array) {
            (ColumnarValue::Array(element_array), ColumnarValue::Array(array_array)) => {
                let mut results = Vec::new();

                // Convert element array to JSON
                let element_array = encode_array(element_array.clone())?;

                // Get array_array as string array
                let string_array = array_array.as_string::<i32>();

                if let Value::Array(element_array) = element_array {
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..string_array.len() {
                        if string_array.is_null(i) {
                            results.push(None);
                        } else {
                            let array_value: Value = from_slice(string_array.value(i).as_bytes())
                                .context(errors::SerdeJsonMessageSnafu)?;

                            let result = Self::array_position(&element_array[i], &array_value);
                            results.push(result);
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::Int64Array::from(results),
                )))
            }
            (ColumnarValue::Scalar(element_scalar), ColumnarValue::Scalar(array_scalar)) => {
                let element_scalar = encode_scalar(element_scalar)?;
                let array_scalar = match array_scalar {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => {
                        from_slice(s.as_bytes()).context(errors::SerdeJsonMessageSnafu)?
                    }
                    _ => {
                        return errors::ArrayArgumentMustBeAStringTypeSnafu.fail()?;
                    }
                };

                let result = Self::array_position(&element_scalar, &array_scalar);
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
            }
            _ => errors::MismatchedArgumentTypesSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayPositionUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_position() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayPositionUDF::new()));

        // Test basic array position
        let sql = "SELECT array_position('hello', array_construct('hello', 'hi')) as result1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result1 |",
                "+---------+",
                "| 0       |",
                "+---------+",
            ],
            &result
        );

        // Test element not found
        let sql = "SELECT array_position('world', array_construct('hello', 'hi')) as result2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result2 |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );

        // Test with null values
        let sql = "SELECT array_position(NULL, array_construct('hello', 'hi')) as result3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result3 |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );

        // Test searching for NULL in array containing NULL
        let sql = "SELECT array_position(NULL, array_construct('hello', NULL, 'hi')) as result4";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result4 |",
                "+---------+",
                "| 1       |",
                "+---------+",
            ],
            &result
        );

        Ok(())
    }
}
