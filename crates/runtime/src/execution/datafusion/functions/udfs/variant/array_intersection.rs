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
use arrow_array::cast::AsArray;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_slice};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayIntersectionUDF {
    signature: Signature,
}

impl ArrayIntersectionUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn array_intersection(
        &self,
        array1_str: Option<&str>,
        array2_str: Option<&str>,
    ) -> DFResult<Option<Value>> {
        if let (Some(arr1), Some(arr2)) = (array1_str, array2_str) {
            // Parse both arrays
            let array1_value: Value = from_slice(arr1.as_bytes()).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to parse first array: {}",
                    e
                ))
            })?;

            let array2_value: Value = from_slice(arr2.as_bytes()).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to parse second array: {}",
                    e
                ))
            })?;

            if let (Value::Array(arr1), Value::Array(arr2)) = (array1_value, array2_value) {
                // Create a new array with elements that exist in both arr1 and arr2
                let result: Vec<Value> = arr2
                    .into_iter()
                    .filter(|item| arr1.contains(item))
                    .collect();

                Ok(Some(Value::Array(result)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayIntersectionUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayIntersectionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_intersection"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array1 = args.first().expect("Expected first array argument");
        let array2 = args.get(1).expect("Expected second array argument");

        match (array1, array2) {
            (ColumnarValue::Array(array1_array), ColumnarValue::Array(array2_array)) => {
                let array1_strings = array1_array.as_string::<i32>();
                let array2_strings = array2_array.as_string::<i32>();
                let mut results = Vec::new();

                for (arr1, arr2) in array1_strings.iter().zip(array2_strings) {
                    let result = self.array_intersection(arr1, arr2)?;
                    results.push(result.map(|v| serde_json::to_string(&v).unwrap()));
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::StringArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(array1_scalar), ColumnarValue::Scalar(array2_scalar)) => {
                let array1_str = match array1_scalar {
                    ScalarValue::Utf8(Some(s)) => s,
                    ScalarValue::Null | ScalarValue::Utf8(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected UTF8 string for first array".to_string(),
                        ));
                    }
                };

                let array2_str = match array2_scalar {
                    ScalarValue::Utf8(Some(s)) => s,
                    ScalarValue::Null | ScalarValue::Utf8(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected UTF8 string for second array".to_string(),
                        ));
                    }
                };

                let result = self.array_intersection(Some(array1_str), Some(array2_str))?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.map(|v| serde_json::to_string(&v).unwrap()),
                )))
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "Mismatched argument types".to_string(),
            )),
        }
    }
}

make_udf_function!(ArrayIntersectionUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_intersection() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test basic array intersection
        let sql = "SELECT array_intersection(array_construct('A', 'B'), array_construct('B', 'C')) as result1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result1 |",
                "+---------+",
                "| [\"B\"]   |",
                "+---------+",
            ],
            &result
        );

        // Test empty intersection
        let sql = "SELECT array_intersection(array_construct('A', 'B'), array_construct('C', 'D')) as result2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result2 |",
                "+---------+",
                "| []      |",
                "+---------+",
            ],
            &result
        );

        // Test with null values
        let sql = "SELECT array_intersection(array_construct('A', NULL), array_construct('A', NULL)) as result3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| result3    |",
                "+------------+",
                "| [\"A\",null] |",
                "+------------+",
            ],
            &result
        );

        // Test with duplicate values
        let sql = "SELECT array_intersection(array_construct('A', 'B', 'B', 'B', 'C'), array_construct('B', 'B')) as result4";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| result4   |",
                "+-----------+",
                "| [\"B\",\"B\"] |",
                "+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
