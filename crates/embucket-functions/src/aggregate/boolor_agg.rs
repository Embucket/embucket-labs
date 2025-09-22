use crate::aggregate::macros::make_udaf_function;
use crate::array_to_boolean;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Accumulator;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use std::any::Any;

/// Boolor function
/// Returns TRUE if at least one Boolean record in a group evaluates to TRUE.
///
/// If all records in the group are NULL, or if the group is empty, the function returns NULL.
///
/// Syntax: `boolor_agg(<expr>)`

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BoolOrAggUDAF {
    signature: Signature,
}

impl Default for BoolOrAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolOrAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BoolOrAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "boolor_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(BoolOrAggAccumulator::new()))
    }
}

#[derive(Debug)]
struct BoolOrAggAccumulator {
    state: Option<bool>,
}

impl BoolOrAggAccumulator {
    pub const fn new() -> Self {
        Self { state: None }
    }
}

impl Accumulator for BoolOrAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        if matches!(self.state, Some(false)) {
            return Ok(());
        }

        let barr = array_to_boolean(&values[0])?;

        let mut non_null = false;
        for val in &barr {
            if val.is_some() {
                non_null = true;
            }
            if matches!(val, Some(true)) {
                self.state = Some(true);
                return Ok(());
            }
        }
        if non_null {
            self.state = Some(false);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::from(self.state))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.state)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }

        if matches!(self.state, Some(true)) {
            return Ok(());
        }
        let mut non_null = false;
        let v = ScalarValue::try_from_array(&states[0], 0)?;
        if !v.is_null() {
            non_null = true;
        }
        if matches!(v, ScalarValue::Boolean(Some(true))) {
            self.state = Some(true);
            return Ok(());
        }

        if non_null {
            self.state = Some(false);
        }

        Ok(())
    }
}

make_udaf_function!(BoolOrAggUDAF);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::BooleanArray;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::AggregateUDF;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_merge() -> DFResult<()> {
        let mut acc = BoolOrAggAccumulator::new();
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(true)]))])?;
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(true)]))])?;
        assert_eq!(acc.state, Some(true));

        let mut acc = BoolOrAggAccumulator::new();
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(true)]))])?;
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(false)]))])?;
        assert_eq!(acc.state, Some(true));

        let mut acc = BoolOrAggAccumulator::new();
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(false)]))])?;
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(false)]))])?;
        assert_eq!(acc.state, Some(false));

        let mut acc = BoolOrAggAccumulator::new();
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(true)]))])?;
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![None]))])?;
        assert_eq!(acc.state, Some(true));

        let mut acc = BoolOrAggAccumulator::new();
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![Some(false)]))])?;
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![None]))])?;
        assert_eq!(acc.state, Some(false));

        let mut acc = BoolOrAggAccumulator::new();
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![None]))])?;
        acc.merge_batch(&[Arc::new(BooleanArray::from(vec![None]))])?;
        assert_eq!(acc.state, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql() -> DFResult<()> {
        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(AggregateUDF::from(BoolOrAggUDAF::new()));
        ctx.sql(
            "create table test_boolean_agg
(
    id integer,
    c  boolean
) as values
    (1, true),
    (1, true),
    (2, true),
    (2, false),
    (3, true),
    (3, null),
    (4, false),
    (4, null),
    (5, null),
    (5, null);",
        )
        .await?;

        let result = ctx
            .sql("select id, boolor_agg(c) from test_boolean_agg group by id order by id;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+----+--------------------------------+",
                "| id | boolor_agg(test_boolean_agg.c) |",
                "+----+--------------------------------+",
                "| 1  | true                           |",
                "| 2  | true                           |",
                "| 3  | true                           |",
                "| 4  | false                          |",
                "| 5  |                                |",
                "+----+--------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
