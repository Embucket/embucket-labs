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
pub struct ObjectInsertUDF {
    signature: Signature,
}

impl ObjectInsertUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn insert_key_value(
        object_value: Value,
        key: &Value,
        value: &Value,
        update_flag: bool,
    ) -> DFResult<Option<String>> {
        // Ensure the first argument is an object
        if let Value::Object(mut obj) = object_value {
            // Get the key string
            let Value::String(key_str) = key else {
                return Err(datafusion_common::error::DataFusionError::Internal(
                    "Key must be a string".to_string(),
                ));
            };

            // Check if key exists and handle according to update_flag
            if obj.contains_key(key_str) && !update_flag {
                return Err(datafusion_common::error::DataFusionError::Internal(
                    format!("Key '{key_str}' already exists and update_flag is false",),
                ));
            }

            // Insert or update the key-value pair
            obj.insert(key_str.clone(), value.clone());

            // Convert back to JSON string
            Ok(Some(to_string(&obj).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {e}",
                ))
            })?))
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON object".to_string(),
            ))
        }
    }
}

impl Default for ObjectInsertUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ObjectInsertUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "object_insert"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(clippy::too_many_lines)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let object_str =
            args.first()
                .ok_or(datafusion_common::error::DataFusionError::Internal(
                    "Expected object argument".to_string(),
                ))?;

        // Get key argument
        let key_arg = args
            .get(1)
            .ok_or(datafusion_common::error::DataFusionError::Internal(
                "Expected key argument".to_string(),
            ))?;

        // Get value argument
        let value_arg = args
            .get(2)
            .ok_or(datafusion_common::error::DataFusionError::Internal(
                "Expected value argument".to_string(),
            ))?;

        // Get update flag (optional)
        let update_flag = args.get(3).map_or(Ok(false), |arg| {
            if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = arg {
                Ok(*b)
            } else {
                Err(datafusion_common::error::DataFusionError::Internal(
                    "Update flag must be a boolean".to_string(),
                ))
            }
        })?;

        // Convert key and value to JSON Values
        let key_json = match key_arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                let key_json = json::encode_array(value.to_array_of_size(1)?)?;
                if let Value::Array(array) = key_json {
                    match array.first() {
                        Some(value) => value.clone(),
                        None => {
                            return Err(datafusion_common::error::DataFusionError::Internal(
                                "Expected array for scalar value".to_string(),
                            ));
                        }
                    }
                } else {
                    return Err(datafusion_common::error::DataFusionError::Internal(
                        "Expected array for scalar value".to_string(),
                    ));
                }
            }
            ColumnarValue::Array(_) => {
                return Err(datafusion_common::error::DataFusionError::Internal(
                    "Key argument must be a scalar value".to_string(),
                ));
            }
        };

        let value_json = match value_arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                let value_json = json::encode_array(value.to_array_of_size(1)?)?;
                if let Value::Array(array) = value_json {
                    match array.first() {
                        Some(value) => value.clone(),
                        None => {
                            return Err(datafusion_common::error::DataFusionError::Internal(
                                "Expected array for scalar value".to_string(),
                            ));
                        }
                    }
                } else {
                    return Err(datafusion_common::error::DataFusionError::Internal(
                        "Expected array for scalar value".to_string(),
                    ));
                }
            }
            ColumnarValue::Array(_) => {
                return Err(datafusion_common::error::DataFusionError::Internal(
                    "Value argument must be a scalar value".to_string(),
                ));
            }
        };

        match object_str {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let object_str = string_array.value(i);
                        let object_json: Value = from_str(object_str).map_err(|e| {
                            datafusion_common::error::DataFusionError::Internal(format!(
                                "Failed to parse object JSON: {e}"
                            ))
                        })?;
                        results.push(Self::insert_key_value(
                            object_json,
                            &key_json,
                            &value_json,
                            update_flag,
                        )?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(object_value) => {
                match object_value {
                    ScalarValue::Utf8(Some(object_str)) => {
                        // Parse object string to JSON Value
                        let object_json: Value = from_str(object_str).map_err(|e| {
                            datafusion_common::error::DataFusionError::Internal(format!(
                                "Failed to parse object JSON: {e}"
                            ))
                        })?;

                        let result = Self::insert_key_value(
                            object_json,
                            &key_json,
                            &value_json,
                            update_flag,
                        )?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
        }
    }
}

make_udf_function!(ObjectInsertUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_object_insert() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDF
        ctx.register_udf(ScalarUDF::from(ObjectInsertUDF::new()));

        // Test inserting new key-value pair
        let sql = "SELECT object_insert('{\"a\": 1, \"b\": 2}', 'c', 3) as inserted";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| inserted            |",
                "+---------------------+",
                "| {\"a\":1,\"b\":2,\"c\":3} |",
                "+---------------------+",
            ],
            &result
        );

        // Test updating existing key with update_flag=true
        let sql = "SELECT object_insert('{\"a\": 1, \"b\": 2}', 'b', 3, true) as updated";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| updated       |",
                "+---------------+",
                "| {\"a\":1,\"b\":3} |",
                "+---------------+",
            ],
            &result
        );

        // Test error when updating existing key without update_flag
        let sql = "SELECT object_insert('{\"a\": 1, \"b\": 2}', 'b', 3) as error";
        let result = ctx.sql(sql).await?.collect().await;
        assert!(result.is_err());

        // Test with NULL input
        let sql = "SELECT object_insert(NULL, 'a', 1) as null_input";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| null_input |",
                "+------------+",
                "|            |",
                "+------------+",
            ],
            &result
        );

        // Test with NULL key
        let sql = "SELECT object_insert('{\"a\": 1}', NULL, 2) as null_key";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| null_key |",
                "+----------+",
                "|          |",
                "+----------+",
            ],
            &result
        );

        // Test with NULL value
        let sql = "SELECT object_insert('{\"a\": 1}', 'b', NULL) as null_value";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| null_value |",
                "+------------+",
                "|            |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
