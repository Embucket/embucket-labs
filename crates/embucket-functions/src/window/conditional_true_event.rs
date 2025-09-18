use crate::array_to_boolean;
use crate::window::macros::make_udwf_function;
use datafusion_common::Result;
use datafusion_common::arrow::array::{Array, ArrayRef, UInt64Builder};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion_expr::{
    Documentation, PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use std::any::Any;
use std::sync::Arc;

/// `CONDITIONAL_TRUE_EVENT` window function
///
/// Returns a window event number for each row within a partition based on a boolean condition.
/// The event number increments whenever the condition transitions from `FALSE` or `NULL` to `TRUE`.
///
/// Syntax: `CONDITIONAL_TRUE_EVENT`(<condition>)
///
/// Arguments:
/// - `<condition>`: Boolean expression evaluated for each row.
///
/// Example:
/// ```sql
/// SELECT CONDITIONAL_TRUE_EVENT(flag) OVER (ORDER BY id) AS evt
/// FROM (VALUES (1, TRUE), (2, TRUE), (3, FALSE), (4, TRUE)) t(id, flag);
/// ```
///
/// Returns:
/// - A non-negative integer representing the number of true events encountered so far in the partition.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ConditionalTrueEvent {
    signature: Signature,
}

impl ConditionalTrueEvent {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Accept either Boolean or any Numeric type
            signature: Signature::one_of(
                vec![
                    TypeSignature::Uniform(1, vec![DataType::Boolean]),
                    TypeSignature::Numeric(1),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ConditionalTrueEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for ConditionalTrueEvent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "conditional_true_event"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<ConditionalTrueEventEvaluator>::default())
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Arc<Field>> {
        Ok(Arc::new(Field::new(field_args.name(), DataType::UInt64, false)))
    }

    fn sort_options(&self) -> Option<datafusion_common::arrow::compute::SortOptions> {
        Some(datafusion_common::arrow::compute::SortOptions {
            descending: false,
            nulls_first: false,
        })
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

#[derive(Debug, Default)]
struct ConditionalTrueEventEvaluator {
    prev: bool,
    count: u64,
}

impl PartitionEvaluator for ConditionalTrueEventEvaluator {
    fn is_causal(&self) -> bool {
        true
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let bools = array_to_boolean(&values[0])?;
        let mut builder = UInt64Builder::with_capacity(num_rows);
        for i in 0..num_rows {
            let current = !Array::is_null(&bools, i) && bools.value(i);
            if current {
                self.count += 1;
            }
            builder.append_value(self.count);
            self.prev = current;
        }
        Ok(Arc::new(builder.finish()))
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<datafusion_common::ScalarValue> {
        let idx = range.end - 1;
        let bools = array_to_boolean(&values[0])?;
        let current = !Array::is_null(&bools, idx) && bools.value(idx);
        if current {
            self.count += 1;
        }
        self.prev = current;
        Ok(datafusion_common::ScalarValue::UInt64(Some(self.count)))
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

make_udwf_function!(ConditionalTrueEvent);
