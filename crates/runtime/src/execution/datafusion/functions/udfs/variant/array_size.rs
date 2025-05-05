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
pub struct ArraySizeUDF {
    signature: Signature,
}

impl ArraySizeUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn get_array_size(&self, value: Value) -> DFResult<Option<i64>> {
        match value {
            Value::Array(array) => Ok(Some(array.len() as i64)),
            _ => Ok(None), // Return NULL for non-array values
        }
    }
}

impl Default for ArraySizeUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraySizeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
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

                        results.push(self.get_array_size(array_json)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::Int64Array::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => match array_value {
                ScalarValue::Utf8(Some(s)) => {
                    let array_json: Value = from_str(s).map_err(|e| {
                        datafusion_common::error::DataFusionError::Internal(format!(
                            "Failed to parse array JSON: {}",
                            e
                        ))
                    })?;

                    let size = self.get_array_size(array_json)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(size)))
                }
                ScalarValue::Utf8(None) | ScalarValue::Null => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)))
                }
                _ => Err(datafusion_common::error::DataFusionError::Internal(
                    "Expected UTF8 string for array".to_string(),
                )),
            },
        }
    }
}

make_udf_function!(ArraySizeUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;
    #[tokio::test]
    async fn test_array_size() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test empty array
        let sql = "SELECT array_size(array_construct()) as empty_size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| empty_size |",
                "+------------+",
                "| 0          |",
                "+------------+",
            ],
            &result
        );

        // Test array with elements
        let sql = "SELECT array_size(array_construct(1, 2, 3, 4)) as size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            ["+------+", "| size |", "+------+", "| 4    |", "+------+",],
            &result
        );

        // Test with non-array input
        let sql = "SELECT array_size('\"not an array\"') as invalid_size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| invalid_size |",
                "+--------------+",
                "|              |",
                "+--------------+",
            ],
            &result
        );

        // Test with NULL input
        let sql = "SELECT array_size(NULL) as null_size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| null_size |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
