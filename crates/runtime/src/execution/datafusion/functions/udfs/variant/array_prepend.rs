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
use serde_json::{Value, from_slice, to_string};
use std::sync::Arc;

use super::json::encode_scalar;

#[derive(Debug, Clone)]
pub struct ArrayPrependUDF {
    signature: Signature,
}

impl ArrayPrependUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn prepend_element(&self, array_value: &Value, element_value: &Value) -> DFResult<String> {
        // Ensure the first argument is an array
        if let Value::Array(array) = array_value {
            // Create new array with element value prepended
            let mut new_array = Vec::with_capacity(array.len() + 1);
            new_array.push(element_value.clone());
            new_array.extend(array.iter().cloned());

            // Convert back to JSON string
            to_string(&Value::Array(new_array)).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {}",
                    e
                ))
            })
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array".to_string(),
            ))
        }
    }
}

impl Default for ArrayPrependUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayPrependUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_prepend"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_str = args.first().expect("Expected array argument");
        let element = args.get(1).expect("Expected element argument");

        match (array_str, element) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(element_value)) => {
                let string_array = array.as_string::<i32>();
                let element_value = encode_scalar(element_value)?;
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_value = from_slice(string_array.value(i).as_bytes())
                            .map_err(|e| datafusion_common::error::DataFusionError::Internal(e.to_string()))?;
                        results.push(Some(self.prepend_element(&array_value, &element_value)?));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(results))))
            }
            (ColumnarValue::Scalar(array_value), ColumnarValue::Scalar(element_value)) => {
                let array_str = match array_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => return Err(datafusion_common::error::DataFusionError::Internal(
                        "Expected UTF8 string for array".to_string()
                    ))
                };
                let array_value = from_slice(array_str.as_bytes())
                    .map_err(|e| datafusion_common::error::DataFusionError::Internal(e.to_string()))?;
                let element_value = encode_scalar(element_value)?;

                let result = self.prepend_element(&array_value, &element_value)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array string, second argument must be a scalar value".to_string()
            ))
        }
    }
}

make_udf_function!(ArrayPrependUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_prepend() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test prepending string to numeric array
        let sql = "SELECT array_prepend(array_construct(0,1,2,3), 'hello') as prepended";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------------+",
                "| prepended         |",
                "+-------------------+",
                "| [\"hello\",0,1,2,3] |",
                "+-------------------+",
            ],
            &result
        );

        // Test prepending number
        let sql = "SELECT array_prepend(array_construct(1,2,3), 42) as num_prepend";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| num_prepend |",
                "+-------------+",
                "| [42,1,2,3]  |",
                "+-------------+",
            ],
            &result
        );

        // Test prepending boolean
        let sql = "SELECT array_prepend(array_construct(1,2,3), true) as bool_prepend";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| bool_prepend |",
                "+--------------+",
                "| [true,1,2,3] |",
                "+--------------+",
            ],
            &result
        );

        // Test prepending null
        let sql = "SELECT array_prepend(array_construct(1,2,3), NULL) as null_prepend";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| null_prepend |",
                "+--------------+",
                "| [null,1,2,3] |",
                "+--------------+",
            ],
            &result
        );

        Ok(())
    }
}
