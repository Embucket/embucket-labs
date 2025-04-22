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

//! `any_value` window function implementation

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::{
    Documentation, PartitionEvaluator, ReversedUDWF, Signature, Volatility, WindowUDFImpl,
};
use datafusion_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use crate::execution::datafusion::functions::window::any_value;
use crate::execution::datafusion::functions::window::macros::make_udwf_function;

make_udwf_function!(AnyValue);


/// Create an expression to represent the `first_value` window function
///
pub fn any_value(arg: datafusion_expr::Expr) -> datafusion_expr::Expr {
    get_udwf().call(vec![arg])
}


#[derive(Debug)]
pub struct AnyValue {
    signature: Signature,
}

impl AnyValue {
    /// Create a new `any_value` function
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for AnyValue {
    fn default() -> Self {
        Self::new()
    }
}

static ANY_VALUE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_any_value_doc() -> &'static Documentation {
    ANY_VALUE_DOCUMENTATION.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_ANALYTICAL,
            "Returns any value from the window frame in a non-deterministic way. \
            Useful for retrieving a sample value when any value from the group would suffice.",
            "any_value(expression)",
        )
            .with_argument("expression", "Expression to return a value from")
            .build()
    })
}

impl WindowUDFImpl for AnyValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "any_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(AnyValueEvaluator {
            state: AnyValueState::default(),
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        let nullable = true;
        let return_type = field_args.input_types().first().unwrap_or(&DataType::Null);

        Ok(Field::new(field_args.name(), return_type.clone(), nullable))
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        ReversedUDWF::Reversed(any_value::get_udwf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_any_value_doc())
    }
}

#[derive(Debug, Default, Clone)]
pub struct AnyValueState {
    pub selected_value: Option<ScalarValue>,
    /// Whether we have finalized the result and can skip further processing
    pub finalized_result: Option<ScalarValue>,
}

#[derive(Debug)]
pub struct AnyValueEvaluator {
    state: AnyValueState,
}

impl PartitionEvaluator for AnyValueEvaluator {
    /// Optimized memoization for ANY_VALUE window function
    ///
    /// When the window frame has a fixed beginning (e.g., UNBOUNDED PRECEDING),
    /// we can memoize the result. Once the result is calculated, it will
    /// always stay the same, so we don't need to keep past data as we process
    /// the entire dataset.
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let out = &state.out_col;
        let size = out.len();


        let is_prunable = size > 0 && self.state.selected_value.is_some();

        if is_prunable && self.state.finalized_result.is_none() {
            // Get the current value and store it as the finalized result
            let result = if let Some(ref value) = self.state.selected_value {
                value.clone()
            } else {
                ScalarValue::try_from_array(out, size - 1)?
            };
            self.state.finalized_result = Some(result);

            state.window_frame_range.start = state.window_frame_range.end.saturating_sub(1);
        }

        Ok(())
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        println!("WE EXECUTED ANY_VALUE");
        if let Some(ref result) = self.state.finalized_result {
            return Ok(result.clone());
        }

        if values.is_empty() || values[0].len() == 0 || range.is_empty() {
            return ScalarValue::try_from(values[0].data_type());
        }

        let arr = &values[0];

        let value = ScalarValue::try_from_array(arr, range.start)?;
        self.state.selected_value = Some(value.clone());
        Ok(value)
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::Int32Array;
    use datafusion_common::arrow::datatypes::Schema;

    #[test]
    fn test_any_value_non_null() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let mut evaluator = AnyValueEvaluator {
            state: AnyValueState::default(),
        };

        let values = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        ];

        let result = evaluator.evaluate(&values, &(0..5))?;
        if let ScalarValue::Int32(val) = result {
            assert!(val.is_some(), "Expected a non-null value");
            let value = val.unwrap();
            assert!(
                (1..=5).contains(&value),
                "ANY_VALUE returned {}, which is not from the window frame (1-5)",
                value
            );
        } else {
            panic!("Expected Int32, got {:?}", result);
        }

        assert!(evaluator.state.selected_value.is_some(), "Should have saved the selected value");

        Ok(())
    }

    #[test]
    fn test_any_value_finalized_result() -> Result<()> {
        let mut evaluator = AnyValueEvaluator {
            state: AnyValueState {
                selected_value: Some(ScalarValue::Int32(Some(42))),
                finalized_result: Some(ScalarValue::Int32(Some(42))),
            },
        };

        let values = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        ];

        let result = evaluator.evaluate(&values, &(0..5))?;

        if let ScalarValue::Int32(val) = result {
            assert_eq!(val, Some(42), "Should return the finalized result");
        } else {
            panic!("Expected Int32, got {:?}", result);
        }

        let result = evaluator.evaluate(&values, &(2..3))?;

        if let ScalarValue::Int32(val) = result {
            assert_eq!(val, Some(42), "Should return the finalized result regardless of range");
        } else {
            panic!("Expected Int32, got {:?}", result);
        }

        Ok(())
    }
}