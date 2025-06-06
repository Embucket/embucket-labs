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
pub struct ObjectDeleteUDF {
    signature: Signature,
}

impl ObjectDeleteUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn delete_keys(object_value: Value, keys: &[Value]) -> DFResult<Option<String>> {
        // Ensure the first argument is an object
        if let Value::Object(mut obj) = object_value {
            // Remove each key from the object
            for key in keys {
                if let Value::String(key_str) = key {
                    obj.remove(key_str);
                }
            }

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

impl Default for ObjectDeleteUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ObjectDeleteUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "object_delete"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let object_str =
            args.first()
                .ok_or(datafusion_common::error::DataFusionError::Internal(
                    "Expected object argument".to_string(),
                ))?;

        // Collect all keys to delete
        let keys: Vec<Value> = args[1..]
            .iter()
            .map(|arg| {
                if let ColumnarValue::Scalar(value) = arg {
                    if value.is_null() {
                        Ok(Value::Null)
                    } else {
                        let key_json = json::encode_array(value.to_array_of_size(1)?)?;
                        if let Value::Array(array) = key_json {
                            match array.first() {
                                Some(value) => Ok(value.clone()),
                                None => Err(datafusion_common::error::DataFusionError::Internal(
                                    "Expected array for scalar value".to_string(),
                                )),
                            }
                        } else {
                            Err(datafusion_common::error::DataFusionError::Internal(
                                "Expected array for scalar value".to_string(),
                            ))
                        }
                    }
                } else {
                    Err(datafusion_common::error::DataFusionError::Internal(
                        "Key arguments must be scalar values".to_string(),
                    ))
                }
            })
            .collect::<DFResult<Vec<Value>>>()?;

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
                        results.push(Self::delete_keys(object_json, &keys)?);
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

                        let result = Self::delete_keys(object_json, &keys)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
        }
    }
}

make_udf_function!(ObjectDeleteUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_object_delete() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDF
        ctx.register_udf(ScalarUDF::from(ObjectDeleteUDF::new()));

        // Test removing single key
        let sql = "SELECT object_delete('{\"a\": 1, \"b\": 2, \"c\": 3}', 'b') as removed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| removed       |",
                "+---------------+",
                "| {\"a\":1,\"c\":3} |",
                "+---------------+",
            ],
            &result
        );

        // Test removing multiple keys
        let sql = "SELECT object_delete('{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4}', 'b', 'd') as removed2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| removed2      |",
                "+---------------+",
                "| {\"a\":1,\"c\":3} |",
                "+---------------+",
            ],
            &result
        );

        // Test removing non-existent key
        let sql = "SELECT object_delete('{\"a\": 1, \"b\": 2}', 'c') as no_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| no_remove     |",
                "+---------------+",
                "| {\"a\":1,\"b\":2} |",
                "+---------------+",
            ],
            &result
        );

        // Test with NULL input
        let sql = "SELECT object_delete(NULL, 'a') as null_input";
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

        Ok(())
    }
}
