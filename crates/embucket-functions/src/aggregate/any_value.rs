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

//! Defines the `ANY_VALUE` aggregation function.

use crate::macros::make_udaf_function;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::get_row_at_idx;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns any value from the group. The function is non-deterministic and can return different values each time it is used. It is typically used to retrieve a sample value when any value from the group would suffice.",
    syntax_example = "any_value(expression)",
    sql_example = r"```sql
> SELECT department, any_value(employee_name) FROM employees GROUP BY department;
+------------+-------------------------+
| department | any_value(employee_name)|
+------------+-------------------------+
| Sales      | John Smith              |
| Engineering| Jane Doe                |
| Marketing  | Sam Johnson             |
+------------+-------------------------+
```",
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash)]
pub struct AnyValue {
    signature: Signature,
}

impl Debug for AnyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AnyValue")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for AnyValue {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AnyValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "any_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AnyValueAccumulator::try_new(
            acc_args.return_type(),
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let fields: Vec<FieldRef> = vec![
            Arc::new(Field::new(
                format_state_name(args.name, "any_value"),
                args.return_type().clone(),
                true,
            )),
            // is_set flag to track whether a value has been set
            Arc::new(Field::new("is_set", DataType::Boolean, true)),
        ];
        Ok(fields)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
pub struct AnyValueAccumulator {
    /// The value to return
    value: ScalarValue,
    /// Whether a value has been set
    is_set: bool,
}

impl AnyValueAccumulator {
    /// Creates a new `AnyValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        ScalarValue::try_from(data_type).map(|value| Self {
            value,
            is_set: false,
        })
    }

    // Updates state with the value in the given row.
    fn update_with_new_row(&mut self, row: &[ScalarValue]) {
        self.value = row[0].clone();
        self.is_set = true;
    }
}

impl Accumulator for AnyValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.is_set {
            return Ok(());
        }

        if !values.is_empty() && !values[0].is_empty() {
            let row = get_row_at_idx(values, 0)?;
            self.update_with_new_row(&row);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.value) + self.value.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::Boolean(Some(self.is_set)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() || states[0].is_empty() {
            return Ok(());
        }

        // The second state contains is_set flags
        let flags = states[1].as_boolean();

        // Find first entry where is_set is true
        for i in 0..flags.len() {
            if flags.value(i) && !self.is_set {
                let value = ScalarValue::try_from_array(&states[0], i)?;
                self.value = value;
                self.is_set = true;
                break;
            }
        }

        Ok(())
    }
}

make_udaf_function!(AnyValue);

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use std::sync::Arc;

    use super::*;

    #[test]
    #[allow(clippy::as_conversions)]
    fn test_any_value() -> Result<()> {
        let mut accumulator = AnyValueAccumulator::try_new(&DataType::Int64)?;
        let arr = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;

        accumulator.update_batch(&[Arc::clone(&arr)])?;

        assert_eq!(accumulator.evaluate()?, ScalarValue::Int64(Some(1)));

        let arr2 = Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef;
        accumulator.update_batch(&[arr2])?;

        assert_eq!(accumulator.evaluate()?, ScalarValue::Int64(Some(1)));

        Ok(())
    }

    #[test]
    #[allow(clippy::as_conversions)]
    fn test_any_value_merge() -> Result<()> {
        let mut acc1 = AnyValueAccumulator::try_new(&DataType::Int64)?;
        let arr1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        acc1.update_batch(&[arr1])?;
        let state1 = acc1.state()?;

        let mut acc2 = AnyValueAccumulator::try_new(&DataType::Int64)?;
        let arr2 = Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef;
        acc2.update_batch(&[arr2])?;
        let state2 = acc2.state()?;

        let val_arrays = vec![
            ScalarValue::iter_to_array(vec![state1[0].clone(), state2[0].clone()])?,
            ScalarValue::iter_to_array(vec![state1[1].clone(), state2[1].clone()])?,
        ];

        let mut merge_acc = AnyValueAccumulator::try_new(&DataType::Int64)?;
        merge_acc.merge_batch(&val_arrays)?;

        let result = merge_acc.evaluate()?;
        if let ScalarValue::Int64(Some(val)) = result {
            assert!(val == 1 || val == 4, "Expected 1 or 4, got {val}");
        } else {
            panic!("Expected Int64, got {result:?}");
        }

        Ok(())
    }
}
