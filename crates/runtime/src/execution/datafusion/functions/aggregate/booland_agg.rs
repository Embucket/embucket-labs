use crate::execution::datafusion::functions::aggregate::macros::make_udaf_function;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use datafusion::common::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Accumulator;
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use std::any::Any;

/// Booland Agg function
/// Returns TRUE if all non-NULL Boolean records in a group evaluate to TRUE.
/// If all records in the group are NULL, or if the group is empty, the function returns NULL.
///
/// Syntax: `booland_agg(<expr>)`

#[derive(Debug, Clone)]
pub struct BoolAndAggUDAF {
    signature: Signature,
}

impl Default for BoolAndAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolAndAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BoolAndAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "booland_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(BoolAndAggAccumulator::new()))
    }
}

#[derive(Debug)]
struct BoolAndAggAccumulator {
    state: Option<bool>,
}

impl BoolAndAggAccumulator {
    pub const fn new() -> Self {
        Self { state: None }
    }
}

impl Accumulator for BoolAndAggAccumulator {
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
            if matches!(val, Some(false)) {
                self.state = Some(false);
                return Ok(());
            }
        }
        if non_null {
            self.state = Some(true);
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

        let mut non_null = false;
        for state in states {
            let v = ScalarValue::try_from_array(state, 0)?;
            if !v.is_null() {
                non_null = true;
            }
            if matches!(v, ScalarValue::Boolean(Some(false))) {
                self.state = Some(false);
                return Ok(());
            }
        }

        if non_null {
            self.state = Some(true);
        }

        Ok(())
    }
}

fn array_to_boolean(arr: &ArrayRef) -> DFResult<BooleanArray> {
    let arr = arr.as_ref();
    let mut boolean_array = BooleanArray::builder(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            boolean_array.append_null();
        } else {
            let b = match arr.data_type() {
                DataType::Boolean => downcast_value!(arr, BooleanArray).value(i),
                DataType::Int8 => downcast_value!(arr, Int8Array).value(i) != 0,
                DataType::Int16 => downcast_value!(arr, Int16Array).value(i) != 0,
                DataType::Int32 => downcast_value!(arr, Int32Array).value(i) != 0,
                DataType::Int64 => downcast_value!(arr, Int64Array).value(i) != 0,
                DataType::UInt8 => downcast_value!(arr, UInt8Array).value(i) != 0,
                DataType::UInt16 => downcast_value!(arr, UInt16Array).value(i) != 0,
                DataType::UInt32 => downcast_value!(arr, UInt32Array).value(i) != 0,
                DataType::UInt64 => downcast_value!(arr, UInt64Array).value(i) != 0,
                DataType::Float32 => downcast_value!(arr, Float32Array).value(i) != 0.,
                DataType::Float64 => downcast_value!(arr, Float64Array).value(i) != 0.,
                DataType::Decimal128(_, _) => downcast_value!(arr, Decimal128Array).value(i) != 0,
                _ => {
                    return Err(DataFusionError::Internal(
                        "only supports boolean, numeric, decimal, float types".to_string(),
                    ))
                }
            };

            boolean_array.append_value(b);
        }
    }
    Ok(boolean_array.finish())
}

make_udaf_function!(BoolAndAggUDAF);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::AggregateUDF;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_merge() -> DFResult<()> {
        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(BooleanArray::from(vec![Some(true)])),
        ])?;
        assert_eq!(acc.state, Some(true));

        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
        ])?;
        assert_eq!(acc.state, Some(false));

        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![Some(false)])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
        ])?;
        assert_eq!(acc.state, Some(false));

        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(BooleanArray::from(vec![None])),
        ])?;
        assert_eq!(acc.state, Some(true));

        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![Some(false)])),
            Arc::new(BooleanArray::from(vec![None])),
        ])?;
        assert_eq!(acc.state, Some(false));

        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![None])),
            Arc::new(BooleanArray::from(vec![None])),
        ])?;
        assert_eq!(acc.state, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql() -> DFResult<()> {
        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(AggregateUDF::from(BoolAndAggUDAF::new()));
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
            .sql("select id, booland_agg(c) from test_boolean_agg group by id order by id;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+----+---------------------------------+",
                "| id | booland_agg(test_boolean_agg.c) |",
                "+----+---------------------------------+",
                "| 1  | true                            |",
                "| 2  | false                           |",
                "| 3  | true                            |",
                "| 4  | false                           |",
                "| 5  |                                 |",
                "+----+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_numeric() -> DFResult<()> {
        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(AggregateUDF::from(BoolAndAggUDAF::new()));
        ctx.sql(
            "create table test_boolean_agg
(
    id integer,
    c  integer
) as values
    (1, 1),
    (1, 1),
    (2, 1),
    (2, 0),
    (3, 1),
    (3, null),
    (4, 0),
    (4, null),
    (5, null),
    (5, null);",
        )
        .await?;

        let result = ctx
            .sql("select id, booland_agg(c) from test_boolean_agg group by id order by id;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+----+---------------------------------+",
                "| id | booland_agg(test_boolean_agg.c) |",
                "+----+---------------------------------+",
                "| 1  | true                            |",
                "| 2  | false                           |",
                "| 3  | true                            |",
                "| 4  | false                           |",
                "| 5  |                                 |",
                "+----+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
