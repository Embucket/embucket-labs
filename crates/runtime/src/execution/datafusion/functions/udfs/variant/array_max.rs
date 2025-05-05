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
use datafusion_common::types::{NativeType, logical_binary, logical_string};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use serde_json::{Value, from_slice};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayMaxUDF {
    signature: Signature,
}

impl ArrayMaxUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Coercible(vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_string()),
                    vec![TypeSignatureClass::Native(logical_binary())],
                    NativeType::String,
                )]),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn find_max(&self, string: impl AsRef<str>) -> DFResult<Option<String>> {
        let string = string.as_ref();
        let array_value: Value =
            from_slice(string.as_bytes()).expect("Couldn't parse the JSON string");

        if let Value::Array(array) = array_value {
            if array.is_empty() {
                return Ok(None);
            }

            // Try to find the maximum value, handling different types
            let mut max_value: Option<String> = None;
            let mut max_type: Option<&str> = None;

            for value in array {
                match value {
                    Value::Number(n) if n.is_i64() => {
                        let num = n.as_i64().unwrap();
                        let should_update = match max_value.as_ref() {
                            None => true,
                            Some(current) => {
                                if let Ok(current_num) = current.parse::<i64>() {
                                    max_type == Some("i64") && num > current_num
                                } else {
                                    false
                                }
                            }
                        };
                        if should_update {
                            max_value = Some(num.to_string());
                            max_type = Some("i64");
                        }
                    }
                    Value::Number(n) if n.is_f64() => {
                        let num = n.as_f64().unwrap();
                        let should_update = match max_value.as_ref() {
                            None => true,
                            Some(current) => {
                                if let Ok(current_num) = current.parse::<f64>() {
                                    max_type == Some("f64") && num > current_num
                                } else {
                                    false
                                }
                            }
                        };
                        if should_update {
                            max_value = Some(num.to_string());
                            max_type = Some("f64");
                        }
                    }
                    _ => continue,
                }
            }

            Ok(max_value)
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayMaxUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayMaxUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_str = args.first().expect("Expected a variant argument");
        match array_str {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let str_value = string_array.value(i);
                        results.push(self.find_max(str_value)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => {
                let array_str = match array_value {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Expected UTF8 string".to_string(),
                        ));
                    }
                };

                let result = self.find_max(array_str)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }
}

make_udf_function!(ArrayMaxUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_max() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test numeric array
        let sql = "SELECT array_max(array_construct(1, 5, 3, 9, 2)) as max_num";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| max_num |",
                "+---------+",
                "| 9       |",
                "+---------+",
            ],
            &result
        );

        // Test mixed types
        let sql = "SELECT array_max(array_construct(1, 'hello', 2.5, 10)) as max_mixed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| max_mixed |",
                "+-----------+",
                "| 10        |",
                "+-----------+",
            ],
            &result
        );

        // Test array of nulls
        let sql = "SELECT array_max(array_construct(NULL, NULL, NULL)) as null_max";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| null_max |",
                "+----------+",
                "|          |",
                "+----------+"
            ],
            &result
        );

        // Test empty array
        let sql = "SELECT array_max(array_construct()) as empty_max";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| empty_max |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
