//! Defines the `PERCENTILE_CONT` aggregation function.

use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_common::{internal_err, not_impl_datafusion_err, not_impl_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::{INTEGERS, NUMERICS};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, Documentation, Signature, TypeSignature,
    Volatility,
};
use datafusion_macros::user_doc;
use datafusion_physical_plan::PhysicalExpr;

use crate::macros::make_udaf_function;

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Return a percentile value based on a continuous distribution of the input column. If no input row lies exactly at the desired percentile, the result is calculated using linear interpolation of the two nearest input values.",
    syntax_example = "percentile_cont(percentile) WITHIN GROUP (ORDER BY expression)",
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash)]
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
    let percentile = match get_scalar_value(expr).map_err(|_| {
        not_impl_datafusion_err!(
            "Percentile value for 'PERCENTILE_CONT' must be a literal, got: {expr}"
        )
    })? {
        ScalarValue::Float32(Some(value)) => f64::from(value),
        ScalarValue::Float64(Some(value)) => value,
        sv => {
            return not_impl_err!(
                "Percentile value for 'PERCENTILE_CONT' must be Float32 or Float64 literal (got data type {})",
                sv.data_type()
            );
        }
    };

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
            .order_bys
            .first()
            .is_some_and(|sort_expr| sort_expr.options.descending);

        let adjusted_percentile = if is_descending {
            1.0 - percentile
        } else {
            percentile
        };

        Ok(Box::new(PercentileContAccumulator::new(
            adjusted_percentile,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let fields: Vec<FieldRef> = vec![
            Arc::new(Field::new(
                format_state_name(args.name, "percentile"),
                DataType::Float64,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "values"),
                args.return_type().clone(),
                true,
            )),
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
    /// Collected values for percentile calculation (all converted to f64)
    values: Vec<f64>,
}

impl PercentileContAccumulator {
    pub const fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }

    #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
    fn value_to_f64(value: &ScalarValue) -> Option<f64> {
        match value {
            ScalarValue::Float64(Some(v)) => Some(*v),
            ScalarValue::Float32(Some(v)) => Some(f64::from(*v)),
            ScalarValue::Int64(Some(v)) => Some(*v as f64),
            ScalarValue::Int32(Some(v)) => Some(f64::from(*v)),
            ScalarValue::Int16(Some(v)) => Some(f64::from(*v)),
            ScalarValue::Int8(Some(v)) => Some(f64::from(*v)),
            ScalarValue::UInt64(Some(v)) => Some(*v as f64),
            ScalarValue::UInt32(Some(v)) => Some(f64::from(*v)),
            ScalarValue::UInt16(Some(v)) => Some(f64::from(*v)),
            ScalarValue::UInt8(Some(v)) => Some(f64::from(*v)),
            _ => None,
        }
    }
}

impl Accumulator for PercentileContAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() || values[0].is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = ScalarValue::try_from_array(array, i)?;
                if let Some(f) = Self::value_to_f64(&value) {
                    self.values.push(f);
                }
            }
        }

        Ok(())
    }

    #[allow(
        clippy::cast_precision_loss,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }

        self.values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if self.values.len() == 1 {
            return Ok(ScalarValue::Float64(Some(self.values[0])));
        }

        let index = self.percentile * (self.values.len() as f64 - 1.0);

        let lower_idx = index.floor() as usize;

        if (index - lower_idx as f64).abs() < f64::EPSILON {
            return Ok(ScalarValue::Float64(Some(self.values[lower_idx])));
        }

        let upper_idx = lower_idx + 1;
        let factor = index - lower_idx as f64;

        let result = (self.values[upper_idx] - self.values[lower_idx])
            .mul_add(factor, self.values[lower_idx]);
        Ok(ScalarValue::Float64(Some(result)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let percentile_value = ScalarValue::Float64(Some(self.percentile));

        let values_value = if self.values.is_empty() {
            ScalarValue::Float64(None)
        } else if self.values.len() == 1 {
            ScalarValue::Float64(Some(self.values[0]))
        } else {
            self.evaluate()?
        };

        Ok(vec![percentile_value, values_value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() || states[0].is_empty() {
            return Ok(());
        }

        let values_array = &states[1];

        for i in 0..values_array.len() {
            if !values_array.is_null(i) {
                let value = ScalarValue::try_from_array(values_array, i)?;
                if let Some(f) = Self::value_to_f64(&value) {
                    self.values.push(f);
                }
            }
        }

        Ok(())
    }
}

make_udaf_function!(PercentileCont);

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_percentile_cont_median() -> Result<()> {
        let mut accumulator = PercentileContAccumulator::new(0.5);

        let values_array: ArrayRef = Arc::new(Int64Array::from(vec![10, 30, 20, 40, 50]));

        accumulator.update_batch(&[values_array])?;
        let result = accumulator.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(Some(30.0)));

        Ok(())
    }

    #[test]
    fn test_percentile_cont_interpolation() -> Result<()> {
        let mut accumulator = PercentileContAccumulator::new(0.75);

        let values_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 30.0, 20.0, 40.0]));

        accumulator.update_batch(&[values_array])?;
        let result = accumulator.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(Some(32.5)));

        Ok(())
    }

    #[test]
    fn test_percentile_cont_integer_to_float() -> Result<()> {
        let mut accumulator = PercentileContAccumulator::new(0.4);

        let values_array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));

        accumulator.update_batch(&[values_array])?;
        let result = accumulator.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(Some(26.0)));

        Ok(())
    }

    #[test]
    fn test_percentile_cont_empty() -> Result<()> {
        let mut accumulator = PercentileContAccumulator::new(0.5);

        let empty_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));

        accumulator.update_batch(&[empty_array])?;
        let result = accumulator.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(None));

        Ok(())
    }
}
