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

//! Defines the `PERCENTILE_CONT` aggregation function.

use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::Array;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::{NUMERICS, INTEGERS};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, ColumnarValue,  Documentation, Signature, Volatility, TypeSignature, Expr};
use datafusion_macros::user_doc;
use datafusion_physical_plan::PhysicalExpr;
use datafusion_common::{internal_err, not_impl_datafusion_err, not_impl_err};

use super::macros::make_udaf_function;




/// Implementation of percentile_cont
#[user_doc(
    doc_section(label = "General Functions"),
    description = "Return a percentile value based on a continuous distribution of the input column. If no input row lies exactly at the desired percentile, the result is calculated using linear interpolation of the two nearest input values.",
    syntax_example = "percentile_cont(percentile) WITHIN GROUP (ORDER BY expression)",
    standard_argument(name = "expression",)
)]
pub struct PercentileCont {
    signature: Signature,
}

impl Debug for PercentileCont {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PercentileCont")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for PercentileCont {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileCont {
    pub fn new() -> Self {
        let mut variants = Vec::with_capacity(NUMERICS.len() * (INTEGERS.len() + 1));
        // Accept any numeric value paired with a float64 percentile
        for num in NUMERICS {
            variants.push(TypeSignature::Exact(vec![num.clone(), DataType::Float64]));
        }
        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }
}

fn get_scalar_value(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    if let ColumnarValue::Scalar(s) = expr.evaluate(&batch)? {
        Ok(s)
    } else {
        internal_err!("Didn't expect ColumnarValue::Array")
    }
}

fn validate_input_percentile_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    let percentile = match get_scalar_value(expr)
        .map_err(|_| not_impl_datafusion_err!("Percentile value for 'PERCENTILE_CONT' must be a literal, got: {expr}"))? {
        ScalarValue::Float32(Some(value)) => {
            value as f64
        }
        ScalarValue::Float64(Some(value)) => {
            value
        }
        sv => {
            return not_impl_err!(
                "Percentile value for 'PERCENTILE_CONT' must be Float32 or Float64 literal (got data type {})",
                sv.data_type()
            )
        }
    };

    // Ensure the percentile is between 0 and 1.
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}

impl AggregateUDFImpl for PercentileCont {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "percentile_cont"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // This is difference in Snowflake and Datafusion
        // Datafusion return type is the same as the input type
        // Snowflake return type is always float64
        Ok(DataType::Float64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Extract the percentile value from the first argument
        let percentile = validate_input_percentile_expr(&acc_args.exprs[1])?;  

        // Handle descending order if specified
        let is_descending = acc_args
            .ordering_req
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false);

        let adjusted_percentile = if is_descending {
            1.0 - percentile
        } else {
            percentile
        };

        Ok(Box::new(PercentileContAccumulator::try_new(
            adjusted_percentile,
            acc_args.return_type,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let fields = vec![
            Field::new(
                format_state_name(args.name, "percentile"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(args.name, "values"),
                args.return_type.clone(),
                true,
            ),
        ];
        Ok(fields)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn supports_null_handling_clause(&self) -> bool {
        false
    }

    fn is_ordered_set_aggregate(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
pub struct PercentileContAccumulator {
    /// The percentile value (0.0 to 1.0)
    percentile: f64,
    /// Collected values for percentile calculation
    values: Vec<ScalarValue>,
    /// The return data type
    return_type: DataType,
}

impl PercentileContAccumulator {
    /// Creates a new accumulator
    pub fn try_new(percentile: f64, return_type: &DataType) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            percentile,
            return_type: return_type.clone(),
        })
    }

    /// Perform linear interpolation between two values
    fn interpolate(&self, lower: &ScalarValue, upper: &ScalarValue, factor: f64) -> Result<ScalarValue> {
        match (lower, upper) {
            (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(u))) => {
                Ok(ScalarValue::Float64(Some(l + (u - l) * factor)))
            }
            (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(u))) => {
                Ok(ScalarValue::Float32(Some(l + (u - l) * factor as f32)))
            }
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(u))) => {
                // If we're doing exact integer interpolation (no fractional part), return Int64
                if factor == 0.0 || factor == 1.0 || (upper == lower) {
                    if factor == 0.0 {
                        Ok(ScalarValue::Int64(Some(*l)))
                    } else {
                        Ok(ScalarValue::Int64(Some(*u)))
                    }
                } else {
                    // Otherwise we need to convert to float for interpolation
                    Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
                }
            }
            (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(u))) => {
                Ok(ScalarValue::Float64(Some(*l as f64 + (*u as f64 - *l as f64) * factor)))
            }
            _ => {
                // Default to float64 for other types
                Ok(ScalarValue::Float64(None))
            }
        }
    }
}

impl Accumulator for PercentileContAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() || values[0].is_empty() {
            return Ok(());
        }

        let array = &values[0];
        
        // Collect non-null values
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = ScalarValue::try_from_array(array, i)?;
                self.values.push(value);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return ScalarValue::try_from(&self.return_type);
        }

        // Sort the values
        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if self.values.len() == 1 {
            // If there's only one value, return it regardless of percentile
            return Ok(self.values[0].clone());
        }

        let index = self.percentile * (self.values.len() as f64 - 1.0);
        let lower_idx = index.floor() as usize;
        
        if (index - lower_idx as f64).abs() < f64::EPSILON {
            // Exact match to an index, return that value
            return Ok(self.values[lower_idx].clone());
        }
        
        // Need to interpolate between two adjacent values
        let upper_idx = lower_idx + 1;
        let factor = index - lower_idx as f64;
        
        // Perform linear interpolation
        self.interpolate(&self.values[lower_idx], &self.values[upper_idx], factor)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.iter().map(|v| v.size()).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Store the percentile value as the first state field
        let percentile_value = ScalarValue::Float64(Some(self.percentile));
        
        // For the second state field, serialize the stored values
        // We'll convert our collected values to a regular scalar value
        // rather than a list, following the pattern from avg implementation
        let values_value = if self.values.is_empty() {
            ScalarValue::try_from(&self.return_type)?
        } else if self.values.len() == 1 {
            // If there's only one value, use it directly
            self.values[0].clone()
        } else {
            // Evaluate our percentile to get the result value
            self.evaluate()?
        };
        
        Ok(vec![
            percentile_value,
            values_value,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() || states[0].is_empty() {
            return Ok(());
        }
        
        // First array contains percentile values (should be the same for all rows)
        // Second array contains the actual values to merge
        let values_array = &states[1];
        
        for i in 0..values_array.len() {
            if !values_array.is_null(i) {
                let value = ScalarValue::try_from_array(values_array, i)?;
                // Add this value to our collection
                self.values.push(value);
            }
        }
        
        Ok(())
    }
}

make_udaf_function!(PercentileCont);

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, Int64Array};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_percentile_cont_median() -> Result<()> {
        let mut accumulator = PercentileContAccumulator::try_new(0.5, &DataType::Int64)?;
        
        // Create array for values - now we only need values, not percentile
        let values_array = Arc::new(Int64Array::from(vec![10, 30, 20, 40, 50])) as ArrayRef;

        accumulator.update_batch(&[values_array])?;
        let result = accumulator.evaluate()?;

        // Median of [10, 20, 30, 40, 50] is 30
        assert_eq!(result, ScalarValue::Int64(Some(30)));

        Ok(())
    }

    #[test]
    fn test_percentile_cont_interpolation() -> Result<()> {
        let mut accumulator = PercentileContAccumulator::try_new(0.75, &DataType::Float64)?;
        
        // Create array for values - only values needed
        let values_array = Arc::new(Float64Array::from(vec![10.0, 30.0, 20.0, 40.0])) as ArrayRef;

        accumulator.update_batch(&[values_array])?;
        let result = accumulator.evaluate()?;

        // Sorted: [10, 20, 30, 40]
        // Index = 0.75 * (4-1) = 2.25
        // Interpolation between values[2]=30 and values[3]=40
        // 30 + (40-30) * 0.25 = 32.5
        assert_eq!(result, ScalarValue::Float64(Some(32.5)));

        Ok(())
    }
}
