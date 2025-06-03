use super::super::macros::make_udf_function;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, to_string};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayCompactUDF {
    signature: Signature,
}

impl ArrayCompactUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn compact_array(array_str: impl AsRef<str>) -> DFResult<String> {
        let array_str = array_str.as_ref();

        // Parse the input array
        let array_value: Value = serde_json::from_str(array_str).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to parse array JSON: {e}",
            ))
        })?;

        // Ensure the input is an array
        if let Value::Array(array) = array_value {
            // Filter out null and undefined values
            let compacted = array.iter().filter(|&v| !v.is_null() && v != &Value::Null);

            // Create a new array with the filtered values
            let compacted_array = Value::Array(compacted.cloned().collect());

            // Convert back to JSON string
            to_string(&compacted_array).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {e}",
                ))
            })
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "Input must be a JSON array".to_string(),
            ))
        }
    }
}

impl Default for ArrayCompactUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayCompactUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_str = args
            .first()
            .ok_or(datafusion_common::error::DataFusionError::Internal(
                "Expected array argument".to_string(),
            ))?;

        match array_str {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_value = string_array.value(i);
                        results.push(Some(Self::compact_array(array_value)?));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return Err(datafusion_common::error::DataFusionError::Internal(
                        "Expected UTF8 string for array".to_string(),
                    ));
                };

                let result = Self::compact_array(array_str)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
        }
    }
}

make_udf_function!(ArrayCompactUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::variant::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_compact() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayCompactUDF::new()));

        // Test compacting array with null values
        let sql = "SELECT array_compact(array_construct(1, null, 3, null, 5)) as compacted";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| compacted |",
                "+-----------+",
                "| [1,3,5]   |",
                "+-----------+",
            ],
            &result
        );

        // Test compacting empty array
        let sql = "SELECT array_compact(array_construct()) as empty_compact";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| empty_compact |",
                "+---------------+",
                "| []            |",
                "+---------------+",
            ],
            &result
        );

        // Test compacting array with mixed types
        let sql =
            "SELECT array_compact(array_construct(1, 'hello', null, 3.14, null)) as mixed_compact";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------------+",
                "| mixed_compact    |",
                "+------------------+",
                "| [1,\"hello\",3.14] |",
                "+------------------+",
            ],
            &result
        );

        Ok(())
    }
}
