use crate::json;
use crate::macros::make_udf_function;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_str, to_string};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayRemoveUDF {
    signature: Signature,
}

impl ArrayRemoveUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn remove_element(
        array_value: Value,
        element_value: Option<Value>,
    ) -> DFResult<Option<String>> {
        // If element is null, return null
        if element_value.is_none() {
            return Ok(None);
        }
        let element_value =
            element_value.ok_or(datafusion_common::error::DataFusionError::Internal(
                "Element value is null".to_string(),
            ))?;

        // Ensure the first argument is an array
        if let Value::Array(array) = array_value {
            // Filter out elements equal to the specified value
            let filtered: Vec<Value> = array.into_iter().filter(|x| x != &element_value).collect();

            // Convert back to JSON string
            Ok(Some(to_string(&filtered).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {e}",
                ))
            })?))
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array".to_string(),
            ))
        }
    }
}

impl Default for ArrayRemoveUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayRemoveUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_remove"
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
        let element = args
            .get(1)
            .ok_or(datafusion_common::error::DataFusionError::Internal(
                "Expected element argument".to_string(),
            ))?;

        match (array_str, element) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(element_value)) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                // Convert element_value to JSON Value once if not null
                let element_json = if element_value.is_null() {
                    None
                } else {
                    let element_json = json::encode_array(element_value.to_array_of_size(1)?)?;
                    if let Value::Array(array) = element_json {
                        match array.first() {
                            Some(value) => Some(value.clone()),
                            None => return Err(datafusion_common::error::DataFusionError::Internal(
                                "Expected array for scalar value".to_string()
                            ))
                        }
                    } else {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected array for scalar value".to_string()
                        ))
                    }
                };

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_str = string_array.value(i);
                        let array_json: Value = from_str(array_str)
                            .map_err(|e| datafusion_common::error::DataFusionError::Internal(
                                format!("Failed to parse array JSON: {e}")
                            ))?;
                        results.push(Self::remove_element(array_json, element_json.clone())?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(results))))
            }
            (ColumnarValue::Scalar(array_value), ColumnarValue::Scalar(element_value)) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return Err(datafusion_common::error::DataFusionError::Internal(
                        "Expected UTF8 string for array".to_string()
                    ));
                };

                // Parse array string to JSON Value
                let array_json: Value = from_str(array_str)
                    .map_err(|e| datafusion_common::error::DataFusionError::Internal(
                        format!("Failed to parse array JSON: {e}")
                    ))?;

                // Convert element to JSON Value if not null
                let element_json = if element_value.is_null() {
                    None
                } else {
                    let element_json = json::encode_array(element_value.to_array_of_size(1)?)?;
                    if let Value::Array(array) = element_json {
                        match array.first() {
                            Some(value) => Some(value.clone()),
                            None => return Err(datafusion_common::error::DataFusionError::Internal(
                                "Expected array for scalar value".to_string()
                            ))
                        }
                    } else {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected array for scalar value".to_string()
                        ))
                    }
                };

                let result = Self::remove_element(array_json, element_json)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array string, second argument must be a scalar value".to_string()
            ))
        }
    }
}

make_udf_function!(ArrayRemoveUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_remove() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayRemoveUDF::new()));

        // Test removing from numeric array
        let sql = "SELECT array_remove(array_construct(2, 5, 7, 5, 1), 5) as removed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| removed |",
                "+---------+",
                "| [2,7,1] |",
                "+---------+",
            ],
            &result
        );

        // Test removing string
        let sql =
            "SELECT array_remove(array_construct('a', 'b', 'c', 'b', 'd'), 'b') as str_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| str_remove    |",
                "+---------------+",
                "| [\"a\",\"c\",\"d\"] |",
                "+---------------+",
            ],
            &result
        );

        // Test removing boolean
        let sql =
            "SELECT array_remove(array_construct(true, false, true, false), true) as bool_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| bool_remove   |",
                "+---------------+",
                "| [false,false] |",
                "+---------------+",
            ],
            &result
        );

        // Test removing non-existent element
        let sql = "SELECT array_remove(array_construct(1, 2, 3), 4) as no_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| no_remove |",
                "+-----------+",
                "| [1,2,3]   |",
                "+-----------+",
            ],
            &result
        );

        // Test removing NULL element
        let sql = "SELECT array_remove(array_construct(1, 2, 3), NULL) as null_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| null_remove |",
                "+-------------+",
                "|             |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
