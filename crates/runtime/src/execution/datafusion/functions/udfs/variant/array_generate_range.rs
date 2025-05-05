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
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, to_string};

#[derive(Debug, Clone)]
pub struct ArrayGenerateRangeUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayGenerateRangeUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                ]),
                volatility: Volatility::Immutable,
            },
            aliases: vec!["generate_range".to_string()],
        }
    }
}

impl Default for ArrayGenerateRangeUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayGenerateRangeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "array_generate_range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() < 2 || args.len() > 3 {
            return Err(datafusion_common::error::DataFusionError::Internal(
                "array_generate_range requires 2 or 3 arguments".to_string(),
            ));
        }

        let mut args = args;
        let step = if args.len() == 3 {
            args.pop().unwrap().into_array(number_rows)?
        } else {
            // Default step is 1
            let default_step = ScalarValue::Int64(Some(1));
            default_step.to_array_of_size(number_rows)?
        };
        let stop = args.pop().unwrap().into_array(number_rows)?;
        let start = args.pop().unwrap().into_array(number_rows)?;

        let mut results = Vec::new();

        for i in 0..number_rows {
            let start_val = if !start.is_null(i) {
                start
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(i)
            } else {
                continue;
            };

            let stop_val = if !stop.is_null(i) {
                stop.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(i)
            } else {
                continue;
            };

            let step_val = if !step.is_null(i) {
                step.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(i)
            } else {
                continue;
            };

            for i in (start_val..stop_val).step_by(step_val as usize) {
                results.push(Value::Number(i.into()));
            }
        }

        let json_str = to_string(&Value::Array(results)).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to serialize JSON: {}",
                e
            ))
        })?;

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str))))
    }
}

make_udf_function!(ArrayGenerateRangeUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_generate_range() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.state().register_udf(get_udf());

        // Test basic range
        let sql = "SELECT array_generate_range(2, 5) as range1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| range1  |",
                "+---------+",
                "| [2,3,4] |",
                "+---------+",
            ],
            &result
        );

        // Test with step
        let sql = "SELECT array_generate_range(5, 25, 10) as range2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------+",
                "| range2 |",
                "+--------+",
                "| [5,15] |",
                "+--------+",
            ],
            &result
        );

        // Test empty range
        let sql = "SELECT array_generate_range(5, 2) as range3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------+",
                "| range3 |",
                "+--------+",
                "| []     |",
                "+--------+"
            ],
            &result
        );

        Ok(())
    }
}
