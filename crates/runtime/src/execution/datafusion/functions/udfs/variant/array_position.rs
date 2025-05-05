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
use super::json::{encode_array, encode_scalar};
use arrow::datatypes::DataType;
use arrow_array::Array;
use arrow_array::cast::AsArray;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_slice};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayPositionUDF {
    signature: Signature,
}

impl ArrayPositionUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn array_position(&self, element: &Value, array: &Value) -> DFResult<Option<i64>> {
        if let Value::Array(arr) = array {
            // Find the position of the element in the array
            for (index, item) in arr.iter().enumerate() {
                if item == element {
                    return Ok(Some(index as i64));
                }
            }
            // Element not found - return NULL instead of -1
            Ok(None)
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayPositionUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayPositionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let element = args.first().expect("Expected element argument");
        let array = args.get(1).expect("Expected array argument");

        match (element, array) {
            (ColumnarValue::Array(element_array), ColumnarValue::Array(array_array)) => {
                let mut results = Vec::new();

                // Convert element array to JSON
                let element_array = encode_array(element_array.clone())?;

                // Get array_array as string array
                let string_array = array_array.as_string::<i32>();

                if let Value::Array(element_array) = element_array {
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..string_array.len() {
                        if string_array.is_null(i) {
                            results.push(None);
                        } else {
                            let array_value: Value = from_slice(string_array.value(i).as_bytes())
                                .map_err(|e| {
                                datafusion_common::error::DataFusionError::Internal(e.to_string())
                            })?;

                            let result = self.array_position(&element_array[i], &array_value)?;
                            results.push(result);
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::Int64Array::from(results),
                )))
            }
            (ColumnarValue::Scalar(element_scalar), ColumnarValue::Scalar(array_scalar)) => {
                let element_scalar = encode_scalar(element_scalar)?;
                let array_scalar = match array_scalar {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => from_slice(s.as_bytes()).map_err(|e| {
                        datafusion_common::error::DataFusionError::Internal(e.to_string())
                    })?,
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Array argument must be a string type".to_string(),
                        ));
                    }
                };

                let result = self.array_position(&element_scalar, &array_scalar)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
            }
            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "Mismatched argument types".to_string(),
            )),
        }
    }
}

make_udf_function!(ArrayPositionUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_position() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test basic array position
        let sql = "SELECT array_position('hello', array_construct('hello', 'hi')) as result1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result1 |",
                "+---------+",
                "| 0       |",
                "+---------+",
            ],
            &result
        );

        // Test element not found
        let sql = "SELECT array_position('world', array_construct('hello', 'hi')) as result2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result2 |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );

        // Test with null values
        let sql = "SELECT array_position(NULL, array_construct('hello', 'hi')) as result3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result3 |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );

        // Test searching for NULL in array containing NULL
        let sql = "SELECT array_position(NULL, array_construct('hello', NULL, 'hi')) as result4";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result4 |",
                "+---------+",
                "| 1       |",
                "+---------+",
            ],
            &result
        );

        Ok(())
    }
}
