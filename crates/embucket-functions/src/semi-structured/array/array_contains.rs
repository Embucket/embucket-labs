use crate::json::{encode_array, encode_scalar};
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
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
pub struct ArrayContainsUDF {
    signature: Signature,
}

impl ArrayContainsUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn contains_value(search_value: &Value, array_str: Option<&str>) -> DFResult<Option<bool>> {
        if let Some(array_str) = array_str {
            // Parse the array
            let array_value: Value =
                from_slice(array_str.as_bytes()).context(errors::FailedToDeserializeJsonSnafu)?;

            if let Value::Array(array) = array_value {
                // If search value is null, check if array contains null
                if search_value.is_null() {
                    if array.iter().any(Value::is_null) {
                        return Ok(Some(true));
                    }
                    return Ok(None);
                }

                // For non-null values, compare each array element
                Ok(Some(array.contains(search_value)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayContainsUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayContainsUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let value = args
            .first()
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "first" }.build())?;
        let array = args
            .get(1)
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "second" }.build())?;

        match (value, array) {
            (ColumnarValue::Array(value_array), ColumnarValue::Array(array_array)) => {
                let array_strings = array_array.as_string::<i32>();
                let value_array = encode_array(value_array.clone())?;
                let mut results = Vec::new();
                for (search_val, col_val) in value_array
                    .as_array()
                    .ok_or_else(|| errors::ArrayArgumentExpectedSnafu.build())?
                    .iter()
                    .zip(array_strings)
                {
                    results.push(Self::contains_value(search_val, col_val)?);
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::BooleanArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(value_scalar), ColumnarValue::Scalar(array_scalar)) => {
                let value_scalar = encode_scalar(value_scalar)?;
                let array_str = match array_scalar {
                    ScalarValue::Utf8(Some(s)) => s,
                    ScalarValue::Null | ScalarValue::Utf8(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                    }
                    _ => {
                        return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                    }
                };

                let result = Self::contains_value(&value_scalar, Some(array_str.as_str()))?;
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
            _ => errors::MismatchedArgumentTypesSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayContainsUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_contains() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayContainsUDF::new()));

        // Test value exists in array
        let sql = "SELECT array_contains('hello', array_construct('hello', 'hi')) as contains";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| contains |",
                "+----------+",
                "| true     |",
                "+----------+",
            ],
            &result
        );

        // Test value not in array
        let sql = "SELECT array_contains('hello', array_construct('hola', 'bonjour')) as contains";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| contains |",
                "+----------+",
                "| false    |",
                "+----------+",
            ],
            &result
        );

        // Test null value
        let sql = "SELECT array_contains(NULL, array_construct('hola', 'bonjour')) as contains";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| contains |",
                "+----------+",
                "|          |",
                "+----------+",
            ],
            &result
        );

        // Test null in array
        let sql = "SELECT array_contains(NULL, array_construct('hola', NULL)) as contains";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| contains |",
                "+----------+",
                "| true     |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array_contains_with_table() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayContainsUDF::new()));

        // Create a table with a value column and an array column
        let sql = "CREATE TABLE test_array_contains AS 
            SELECT 
                CAST(value AS VARCHAR) as value,
                array_construct('apple', 'banana', 'orange') as fruits
            FROM (VALUES 
                ('apple'),
                ('grape'),
                ('banana'),
                (NULL)
            ) as t(value)";

        ctx.sql(sql).await?.collect().await?;

        // Test array_contains with table columns
        let sql = "SELECT value, fruits, array_contains(value, fruits) as contains 
                  FROM test_array_contains";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------+-----------------------------+----------+",
                "| value  | fruits                      | contains |",
                "+--------+-----------------------------+----------+",
                "| apple  | [\"apple\",\"banana\",\"orange\"] | true     |",
                "| grape  | [\"apple\",\"banana\",\"orange\"] | false    |",
                "| banana | [\"apple\",\"banana\",\"orange\"] | true     |",
                "|        | [\"apple\",\"banana\",\"orange\"] |          |",
                "+--------+-----------------------------+----------+",
            ],
            &result
        );

        Ok(())
    }
}
