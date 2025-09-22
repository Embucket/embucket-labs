use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_str, to_string};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayReverseUDF {
    signature: Signature,
}

impl ArrayReverseUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn reverse_array(array_value: Value) -> DFResult<Option<String>> {
        // Ensure the argument is an array
        if let Value::Array(mut array) = array_value {
            // Reverse the array
            array.reverse();

            // Convert back to JSON string
            Ok(Some(
                to_string(&array).context(errors::FailedToSerializeValueSnafu)?,
            ))
        } else {
            errors::ArgumentMustBeJsonArraySnafu { argument: "" }.fail()?
        }
    }
}

impl Default for ArrayReverseUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayReverseUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_arg = args
            .first()
            .ok_or_else(|| errors::ArrayArgumentExpectedSnafu.build())?;

        match array_arg {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_str = string_array.value(i);
                        let array_json: Value =
                            from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                        results.push(Self::reverse_array(array_json)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => {
                // If array is NULL, return NULL
                if array_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                };

                // Parse array string to JSON Value
                let array_json: Value =
                    from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                let result = Self::reverse_array(array_json)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }
}

make_udf_function!(ArrayReverseUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_reverse() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayReverseUDF::new()));

        // Test basic array reverse
        let sql = "SELECT array_reverse(array_construct(1, 2, 3, 4)) as reversed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| reversed  |",
                "+-----------+",
                "| [4,3,2,1] |",
                "+-----------+",
            ],
            &result
        );

        // Test with strings
        let sql = "SELECT array_reverse(array_construct('a', 'b', 'c')) as str_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| str_reverse   |",
                "+---------------+",
                "| [\"c\",\"b\",\"a\"] |",
                "+---------------+",
            ],
            &result
        );

        // Test with booleans
        let sql = "SELECT array_reverse(array_construct(true, false, true)) as bool_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------------+",
                "| bool_reverse      |",
                "+-------------------+",
                "| [true,false,true] |",
                "+-------------------+",
            ],
            &result
        );

        // Test with empty array
        let sql = "SELECT array_reverse(array_construct()) as empty_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| empty_reverse |",
                "+---------------+",
                "| []            |",
                "+---------------+",
            ],
            &result
        );

        // Test with NULL
        let sql = "SELECT array_reverse(NULL) as null_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| null_reverse |",
                "+--------------+",
                "|              |",
                "+--------------+",
            ],
            &result
        );

        Ok(())
    }
}
