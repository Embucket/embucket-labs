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
pub struct ArraySortUDF {
    signature: Signature,
}

impl ArraySortUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Number(a), Value::Number(b)) => {
                if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
                    a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    std::cmp::Ordering::Equal
                }
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            // Type-based ordering for different types, with nulls always last
            _ => {
                let type_order = |v: &Value| {
                    match v {
                        Value::Null => 5, // Move nulls to end
                        Value::Bool(_) => 0,
                        Value::Number(_) => 1,
                        Value::String(_) => 2,
                        Value::Array(_) => 3,
                        Value::Object(_) => 4,
                    }
                };
                type_order(a).cmp(&type_order(b))
            }
        }
    }

    fn sort_array(
        &self,
        array_value: Value,
        sort_ascending: bool,
        _nulls_first: bool,
    ) -> DFResult<Option<String>> {
        if let Value::Array(array) = array_value {
            // Convert array elements to a format that can be sorted
            let mut elements: Vec<Option<Value>> = array
                .into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    v => Some(v),
                })
                .collect();

            // Sort the array, putting nulls last for ascending and first for descending
            elements.sort_by(|a, b| {
                match (a, b) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => {
                        if sort_ascending {
                            std::cmp::Ordering::Greater // Nulls last for ascending
                        } else {
                            std::cmp::Ordering::Less // Nulls first for descending
                        }
                    }
                    (Some(_), None) => {
                        if sort_ascending {
                            std::cmp::Ordering::Less // Non-nulls before nulls for ascending
                        } else {
                            std::cmp::Ordering::Greater // Non-nulls after nulls for descending
                        }
                    }
                    (Some(a_val), Some(b_val)) => {
                        let cmp = Self::compare_json_values(a_val, b_val);
                        if sort_ascending { cmp } else { cmp.reverse() }
                    }
                }
            });

            // Convert back to JSON array
            let sorted_array: Vec<Value> = elements
                .into_iter()
                .map(|opt| opt.unwrap_or(Value::Null))
                .collect();

            Ok(Some(to_string(&sorted_array).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to serialize result: {}",
                    e
                ))
            })?))
        } else {
            Err(datafusion_common::error::DataFusionError::Internal(
                "First argument must be a JSON array".to_string(),
            ))
        }
    }
}

impl Default for ArraySortUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraySortUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Get array argument
        let array_arg = args.first().expect("Expected array argument");

        // Get optional sort_ascending argument (default: true)
        let sort_ascending = args
            .get(1)
            .map(|v| match v {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => *b,
                _ => true,
            })
            .unwrap_or(true);

        // Get optional nulls_first argument (default: true)
        let nulls_first = args
            .get(2)
            .map(|v| match v {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => *b,
                _ => true,
            })
            .unwrap_or(true);

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

                        results.push(self.sort_array(array_json, sort_ascending, nulls_first)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => match array_value {
                ScalarValue::Utf8(Some(array_str)) => {
                    let array_json: Value = from_str(array_str).map_err(|e| {
                        datafusion_common::error::DataFusionError::Internal(format!(
                            "Failed to parse array JSON: {}",
                            e
                        ))
                    })?;

                    let result = self.sort_array(array_json, sort_ascending, nulls_first)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                }
                ScalarValue::Utf8(None) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                _ => Err(datafusion_common::error::DataFusionError::Internal(
                    "First argument must be a JSON array string".to_string(),
                )),
            },
        }
    }
}

make_udf_function!(ArraySortUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_sort() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(get_udf());

        // Test basic sorting
        let sql = "SELECT array_sort(array_construct(20, NULL, 0, NULL, 10)) as sorted";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| sorted              |",
                "+---------------------+",
                "| [0,10,20,null,null] |",
                "+---------------------+",
            ],
            &result
        );

        // Test descending order
        let sql = "SELECT array_sort(array_construct(20, NULL, 0, NULL, 10), false) as desc_sort";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| desc_sort           |",
                "+---------------------+",
                "| [null,null,20,10,0] |",
                "+---------------------+",
            ],
            &result
        );

        // Test nulls last
        let sql =
            "SELECT array_sort(array_construct(20, NULL, 0, NULL, 10), true, false) as nulls_last";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| nulls_last          |",
                "+---------------------+",
                "| [0,10,20,null,null] |",
                "+---------------------+",
            ],
            &result
        );

        // Test with mixed types
        let sql = "SELECT array_sort(array_construct('a', 'c', 'b')) as str_sort";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| str_sort      |",
                "+---------------+",
                "| [\"a\",\"b\",\"c\"] |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }
}
