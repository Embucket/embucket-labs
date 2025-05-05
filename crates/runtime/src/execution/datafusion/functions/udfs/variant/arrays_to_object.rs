// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::macros::make_udf_function;
use arrow::datatypes::DataType;
use arrow_array::Array;
use arrow_array::cast::AsArray;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArraysToObjectUDF {
    signature: Signature,
}

impl ArraysToObjectUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn create_object(&self, keys: &[Option<String>], values: &[Value]) -> DFResult<Option<String>> {
        if keys.len() != values.len() {
            return Ok(None);
        }

        let mut obj = serde_json::Map::new();

        for (key_opt, value) in keys.iter().zip(values.iter()) {
            if let Some(key) = key_opt {
                obj.insert(key.clone(), value.clone());
            }
        }

        Ok(Some(json!(obj).to_string()))
    }
}

impl Default for ArraysToObjectUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraysToObjectUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_to_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let keys_arg = args.first().expect("Expected keys array argument");
        let values_arg = args.get(1).expect("Expected values array argument");

        match (keys_arg, values_arg) {
            (ColumnarValue::Array(keys_array), ColumnarValue::Array(values_array)) => {
                let keys_string_array = keys_array.as_string::<i32>();
                let values_string_array = values_array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..keys_string_array.len() {
                    if keys_string_array.is_null(i) && values_string_array.is_null(i) {
                        results.push(None);
                        continue;
                    }

                    let keys: Vec<Option<String>> = (0..keys_string_array.len())
                        .map(|j| {
                            if keys_string_array.is_null(j) {
                                None
                            } else {
                                Some(keys_string_array.value(j).to_string())
                            }
                        })
                        .collect();

                    let values: Vec<Value> = (0..values_string_array.len())
                        .map(|j| {
                            if values_string_array.is_null(j) {
                                Value::Null
                            } else {
                                // Try to parse as JSON, fallback to string if not valid JSON
                                match serde_json::from_str(values_string_array.value(j)) {
                                    Ok(val) => val,
                                    Err(_) => {
                                        Value::String(values_string_array.value(j).to_string())
                                    }
                                }
                            }
                        })
                        .collect();

                    results.push(self.create_object(&keys, &values)?);
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::StringArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(keys_value), ColumnarValue::Scalar(values_value)) => {
                // Handle NULL inputs
                if keys_value.is_null() || values_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let keys_str = match keys_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };

                let values_str = match values_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };

                // Parse arrays
                let keys: Value = serde_json::from_str(keys_str).map_err(|e| {
                    datafusion_common::error::DataFusionError::Internal(format!(
                        "Failed to parse keys JSON array: {}",
                        e
                    ))
                })?;

                let values: Value = serde_json::from_str(values_str).map_err(|e| {
                    datafusion_common::error::DataFusionError::Internal(format!(
                        "Failed to parse values JSON array: {}",
                        e
                    ))
                })?;

                if let (Value::Array(key_array), Value::Array(value_array)) = (keys, values) {
                    let keys: Vec<Option<String>> = key_array
                        .into_iter()
                        .map(|v| match v {
                            Value::String(s) => Some(s),
                            Value::Null => None,
                            _ => Some(v.to_string()),
                        })
                        .collect();

                    let result = self.create_object(&keys, &value_array)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                } else {
                    Err(datafusion_common::error::DataFusionError::Internal(
                        "Both arguments must be JSON arrays".to_string(),
                    ))
                }
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "Arguments must be arrays".to_string(),
            )),
        }
    }
}

make_udf_function!(ArraysToObjectUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_arrays_to_object() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test basic key-value mapping
        let sql = "SELECT arrays_to_object(array_construct('key1', 'key2', 'key3'), array_construct(1, 2, 3)) as obj";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------------------------+",
                "| obj                          |",
                "+------------------------------+",
                "| {\"key1\":1,\"key2\":2,\"key3\":3} |",
                "+------------------------------+",
            ],
            &result
        );

        // Test with NULL key
        let sql = "SELECT arrays_to_object(array_construct('key1', NULL, 'key3'), array_construct(1, 2, 3)) as obj_null_key";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| obj_null_key        |",
                "+---------------------+",
                "| {\"key1\":1,\"key3\":3} |",
                "+---------------------+",
            ],
            &result
        );

        // Test with NULL value
        let sql = "SELECT arrays_to_object(array_construct('key1', 'key2', 'key3'), array_construct(1, NULL, 3)) as obj_null_value";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------------------+",
                "| obj_null_value                  |",
                "+---------------------------------+",
                "| {\"key1\":1,\"key2\":null,\"key3\":3} |",
                "+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
