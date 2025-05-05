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
use serde_json::{Value, to_string};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayAppendUDF {
    signature: Signature,
}

impl ArrayAppendUDF {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn append_element(
        &self,
        array_str: impl AsRef<str>,
        element: &ScalarValue,
    ) -> DFResult<String> {
        let array_str = array_str.as_ref();

        // Parse the input array
        let mut array_value: Value = serde_json::from_str(array_str).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to parse array JSON: {e}",
            ))
        })?;

        let scalar_value = super::json::encode_array(element.to_array_of_size(1)?)?;

        let scalar_value = if let Value::Array(array) = scalar_value {
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
        };
        // Ensure the first argument is an array
        if let Value::Array(ref mut array) = array_value {
            array.push(scalar_value);

            // Convert back to JSON string
            to_string(&array_value).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {e}",
                ))
            })
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array".to_string(),
            ))
        }
    }
}

impl Default for ArrayAppendUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayAppendUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_append"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_str = args.first()
            .ok_or(datafusion_common::error::DataFusionError::Internal("Expected array argument".to_string()))?;
        let element = args.get(1)
            .ok_or(datafusion_common::error::DataFusionError::Internal("Expected element argument".to_string()))?;

        match (array_str, element) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(element_value)) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_value = string_array.value(i);
                        results.push(Some(self.append_element(array_value, element_value)?));
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

                let result = self.append_element(array_str, element_value)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array string, second argument must be a scalar value".to_string()
            ))
        }
    }
}

make_udf_function!(ArrayAppendUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_append() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test appending to numeric array
        let sql = "SELECT array_append(array_construct(1, 2, 3), 4) as appended";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| appended  |",
                "+-----------+",
                "| [1,2,3,4] |",
                "+-----------+",
            ],
            &result
        );

        // Test appending to empty array
        let sql = "SELECT array_append(array_construct(), 1) as empty_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| empty_append |",
                "+--------------+",
                "| [1]          |",
                "+--------------+",
            ],
            &result
        );

        // Test appending string to numeric array
        let sql = "SELECT array_append(array_construct(1, 2), 'hello') as mixed_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| mixed_append  |",
                "+---------------+",
                "| [1,2,\"hello\"] |",
                "+---------------+",
            ],
            &result
        );

        // Test appending boolean
        let sql = "SELECT array_append(array_construct(1, 2), true) as bool_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| bool_append |",
                "+-------------+",
                "| [1,2,true]  |",
                "+-------------+",
            ],
            &result
        );

        // Test appending float
        let sql = "SELECT array_append(array_construct(1, 2), 3.14) as float_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| float_append |",
                "+--------------+",
                "| [1,2,3.14]   |",
                "+--------------+",
            ],
            &result
        );

        // Test appending null
        let sql = "SELECT array_append(array_construct(1, 2), NULL) as null_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| null_append |",
                "+-------------+",
                "| [1,2,null]  |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
