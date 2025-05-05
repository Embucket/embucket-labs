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
use serde_json::{Value, from_str};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArraysOverlapUDF {
    signature: Signature,
}

impl ArraysOverlapUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn arrays_have_overlap(&self, array1: Value, array2: Value) -> DFResult<Option<bool>> {
        // Ensure both arguments are arrays
        if let (Value::Array(arr1), Value::Array(arr2)) = (array1, array2) {
            // Convert arrays to HashSet for efficient comparison
            let set1: std::collections::HashSet<String> =
                arr1.iter().map(|v| v.to_string()).collect();

            // Check if any element from arr2 exists in set1
            for val in arr2.iter() {
                if set1.contains(&val.to_string()) {
                    return Ok(Some(true));
                }
            }

            Ok(Some(false))
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "Both arguments must be JSON arrays".to_string(),
            ))
        }
    }
}

impl Default for ArraysOverlapUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraysOverlapUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_overlap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array1_arg = args.first().expect("Expected first array argument");
        let array2_arg = args.get(1).expect("Expected second array argument");

        match (array1_arg, array2_arg) {
            (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                let string_array1 = array1.as_string::<i32>();
                let string_array2 = array2.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array1.len() {
                    if string_array1.is_null(i) || string_array2.is_null(i) {
                        results.push(None);
                    } else {
                        let array1_str = string_array1.value(i);
                        let array2_str = string_array2.value(i);

                        let array1_json: Value = from_str(array1_str).map_err(|e| {
                            datafusion_common::error::DataFusionError::Internal(format!(
                                "Failed to parse first array JSON: {}",
                                e
                            ))
                        })?;

                        let array2_json: Value = from_str(array2_str).map_err(|e| {
                            datafusion_common::error::DataFusionError::Internal(format!(
                                "Failed to parse second array JSON: {}",
                                e
                            ))
                        })?;

                        results.push(self.arrays_have_overlap(array1_json, array2_json)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::BooleanArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(array1_value), ColumnarValue::Scalar(array2_value)) => {
                // If either array is NULL, return NULL
                if array1_value.is_null() || array2_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                let array1_str = match array1_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected UTF8 string for first array".to_string(),
                        ));
                    }
                };

                let array2_str = match array2_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected UTF8 string for second array".to_string(),
                        ));
                    }
                };

                // Parse array strings to JSON Values
                let array1_json: Value = from_str(array1_str).map_err(|e| {
                    datafusion_common::error::DataFusionError::Internal(format!(
                        "Failed to parse first array JSON: {}",
                        e
                    ))
                })?;

                let array2_json: Value = from_str(array2_str).map_err(|e| {
                    datafusion_common::error::DataFusionError::Internal(format!(
                        "Failed to parse second array JSON: {}",
                        e
                    ))
                })?;

                let result = self.arrays_have_overlap(array1_json, array2_json)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "Both arguments must be JSON array strings".to_string(),
            )),
        }
    }
}

make_udf_function!(ArraysOverlapUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_arrays_overlap() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test with string arrays that overlap
        let sql = "SELECT arrays_overlap(array_construct('hello', 'aloha'), array_construct('hello', 'hi', 'hey')) as overlap";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| overlap |",
                "+---------+",
                "| true    |",
                "+---------+",
            ],
            &result
        );

        // Test with string arrays that don't overlap
        let sql = "SELECT arrays_overlap(array_construct('hello', 'aloha'), array_construct('hola', 'bonjour', 'ciao')) as overlap";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| overlap |",
                "+---------+",
                "| false   |",
                "+---------+",
            ],
            &result
        );

        // Test with NULL values
        let sql = "SELECT arrays_overlap(NULL, array_construct(1, 2, 3)) as overlap";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| overlap |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );

        Ok(())
    }
}
