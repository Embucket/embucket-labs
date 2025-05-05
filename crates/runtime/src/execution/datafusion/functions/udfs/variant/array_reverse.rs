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
use serde_json::{Value, from_str, to_string};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayReverseUDF {
    signature: Signature,
}

impl ArrayReverseUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn reverse_array(&self, array_value: Value) -> DFResult<Option<String>> {
        // Ensure the argument is an array
        if let Value::Array(mut array) = array_value {
            // Reverse the array
            array.reverse();

            // Convert back to JSON string
            Ok(Some(to_string(&array).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {}",
                    e
                ))
            })?))
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "Argument must be a JSON array".to_string(),
            ))
        }
    }
}

impl Default for ArrayReverseUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayReverseUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_arg = args.first().expect("Expected array argument");

        match array_arg {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_str = string_array.value(i);
                        let array_json: Value = from_str(array_str).map_err(|e| {
                            datafusion_common::error::DataFusionError::Internal(format!(
                                "Failed to parse array JSON: {}",
                                e
                            ))
                        })?;

                        results.push(self.reverse_array(array_json)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => {
                // If array is NULL, return NULL
                if array_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let array_str = match array_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected UTF8 string for array".to_string(),
                        ));
                    }
                };

                // Parse array string to JSON Value
                let array_json: Value = from_str(array_str).map_err(|e| {
                    datafusion_common::error::DataFusionError::Internal(format!(
                        "Failed to parse array JSON: {}",
                        e
                    ))
                })?;

                let result = self.reverse_array(array_json)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }
}

make_udf_function!(ArrayReverseUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_reverse() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(get_udf());
        ctx.state().register_udf(array_construct::get_udf());

        // Test basic array reverse
        let sql = "SELECT array_reverse(array_construct(1, 2, 3, 4)) as reversed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| reversed  |",
                "+-----------+",
                "| [4,3,2,1] |",
                "+-----------+",
            ],
            &result
        );

        // Test with strings
        let sql = "SELECT array_reverse(array_construct('a', 'b', 'c')) as str_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| str_reverse   |",
                "+---------------+",
                "| [\"c\",\"b\",\"a\"] |",
                "+---------------+",
            ],
            &result
        );

        // Test with booleans
        let sql = "SELECT array_reverse(array_construct(true, false, true)) as bool_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------------+",
                "| bool_reverse      |",
                "+-------------------+",
                "| [true,false,true] |",
                "+-------------------+",
            ],
            &result
        );

        // Test with empty array
        let sql = "SELECT array_reverse(array_construct()) as empty_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| empty_reverse |",
                "+---------------+",
                "| []            |",
                "+---------------+",
            ],
            &result
        );

        // Test with NULL
        let sql = "SELECT array_reverse(NULL) as null_reverse";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| null_reverse |",
                "+--------------+",
                "|              |",
                "+--------------+",
            ],
            &result
        );

        Ok(())
    }
}
