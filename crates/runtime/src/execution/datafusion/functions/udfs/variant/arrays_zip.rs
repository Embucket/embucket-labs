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
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArraysZipUDF {
    signature: Signature,
}

impl ArraysZipUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn zip_arrays(&self, arrays: Vec<Value>) -> DFResult<Option<String>> {
        // If any array is null, return null
        if arrays.iter().any(|arr| arr.is_null()) {
            return Ok(None);
        }

        // Ensure all inputs are arrays
        let arrays: Vec<Vec<Value>> = arrays
            .into_iter()
            .map(|val| match val {
                Value::Array(arr) => Ok(arr),
                _ => Err(datafusion_common::error::DataFusionError::Internal(
                    "All arguments must be arrays".to_string(),
                )),
            })
            .collect::<DFResult<_>>()?;

        // Find the maximum length among all arrays
        let max_len = arrays.iter().map(|arr| arr.len()).max().unwrap_or(0);

        // Create the zipped array
        let mut result = Vec::with_capacity(max_len);
        for i in 0..max_len {
            let mut obj = serde_json::Map::new();
            for (array_idx, array) in arrays.iter().enumerate() {
                let key = format!("${}", array_idx + 1);
                let value = array.get(i).cloned().unwrap_or(Value::Null);
                obj.insert(key, value);
            }
            result.push(Value::Object(obj));
        }

        Ok(Some(serde_json::to_string(&result).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to serialize result: {}",
                e
            ))
        })?))
    }
}

impl Default for ArraysZipUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraysZipUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return Err(datafusion_common::error::DataFusionError::Internal(
                "ARRAYS_ZIP requires at least one array argument".to_string(),
            ));
        }

        match args.first().unwrap() {
            ColumnarValue::Array(first_array) => {
                let string_array = first_array.as_string::<i32>();
                let mut results = Vec::new();

                for row in 0..string_array.len() {
                    let mut row_arrays = Vec::new();
                    let mut has_null = false;

                    // Collect all array values for this row
                    for arg in &args {
                        match arg {
                            ColumnarValue::Array(arr) => {
                                let arr = arr.as_string::<i32>();
                                if arr.is_null(row) {
                                    has_null = true;
                                    break;
                                }
                                let array_json: Value = serde_json::from_str(arr.value(row))
                                    .map_err(|e| {
                                        datafusion_common::error::DataFusionError::Internal(
                                            format!("Failed to parse array JSON: {}", e),
                                        )
                                    })?;
                                row_arrays.push(array_json);
                            }
                            _ => {
                                return Err(datafusion_common::error::DataFusionError::Internal(
                                    "All arguments must be arrays".to_string(),
                                ));
                            }
                        }
                    }

                    if has_null {
                        results.push(None);
                    } else {
                        results.push(self.zip_arrays(row_arrays)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(_first_value) => {
                let mut scalar_arrays = Vec::new();

                // If any scalar is NULL, return NULL
                for arg in &args {
                    match arg {
                        ColumnarValue::Scalar(scalar) => {
                            if scalar.is_null() {
                                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                            }
                            if let ScalarValue::Utf8(Some(s)) = scalar {
                                let array_json: Value = serde_json::from_str(s).map_err(|e| {
                                    datafusion_common::error::DataFusionError::Internal(format!(
                                        "Failed to parse array JSON: {}",
                                        e
                                    ))
                                })?;
                                scalar_arrays.push(array_json);
                            } else {
                                return Err(datafusion_common::error::DataFusionError::Internal(
                                    "Expected UTF8 string for array".to_string(),
                                ));
                            }
                        }
                        _ => {
                            return Err(datafusion_common::error::DataFusionError::Internal(
                                "Mixed scalar and array arguments are not supported".to_string(),
                            ));
                        }
                    }
                }

                let result = self.zip_arrays(scalar_arrays)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }
}

make_udf_function!(ArraysZipUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_arrays_zip() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test basic zipping of three arrays
        let sql = "SELECT arrays_zip(
            array_construct(1, 2, 3),
            array_construct('first', 'second', 'third'),
            array_construct('i', 'ii', 'iii')
        ) as zipped_arrays";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------------------------------------------------------------------------------------------+",
                "| zipped_arrays                                                                                      |",
                "+----------------------------------------------------------------------------------------------------+",
                "| [{\"$1\":1,\"$2\":\"first\",\"$3\":\"i\"},{\"$1\":2,\"$2\":\"second\",\"$3\":\"ii\"},{\"$1\":3,\"$2\":\"third\",\"$3\":\"iii\"}] |",
                "+----------------------------------------------------------------------------------------------------+",
            ],
            &result
        );

        // Test arrays of different lengths
        let sql = "SELECT arrays_zip(
            array_construct(1, 2, 3),
            array_construct('a', 'b')
        ) as diff_lengths";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------------------------------------------------+",
                "| diff_lengths                                             |",
                "+----------------------------------------------------------+",
                "| [{\"$1\":1,\"$2\":\"a\"},{\"$1\":2,\"$2\":\"b\"},{\"$1\":3,\"$2\":null}] |",
                "+----------------------------------------------------------+",
            ],
            &result
        );

        // Test with NULL array
        let sql = "SELECT arrays_zip(NULL, array_construct(1, 2, 3)) as null_array";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| null_array |",
                "+------------+",
                "|            |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
