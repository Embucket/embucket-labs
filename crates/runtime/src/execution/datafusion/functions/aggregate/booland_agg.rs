use datafusion::common::DataFusionError;
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Accumulator;
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use std::any::Any;

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

    fn name(&self) -> &str {
        "booland_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(BoolAndAggAccumulator::new()))
    }
}

#[derive(Debug)]
struct BoolAndAggAccumulator {
    state: Option<bool>,
}

impl BoolAndAggAccumulator {
    pub fn new() -> Self {
        Self { state: None }
    }
}

impl Accumulator for BoolAndAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        dbg!(&values[0]);
        if values.is_empty() {
            return Ok(());
        }
        if matches!(self.state, Some(false)) {
            return Ok(());
        }

        let arr = &values[0];

        match arr.data_type() {
            DataType::Boolean => {
                let barr = downcast_value!(arr, BooleanArray);
                let mut non_null = false;
                for val in barr {
                    if val.is_some() {
                        non_null = true;
                    }
                    if matches!(val, Some(false)) {
                        println!("!! false");
                        self.state = Some(false);
                        return Ok(());
                    }
                }
                if non_null {
                    self.state = Some(true);
                }
            }
            _ => {
                unimplemented!()
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        dbg!(&self.state);
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
            let barr = downcast_value!(state, BooleanArray);
            for val in barr {
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
        }
        
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::print_batches;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::AggregateUDF;

    #[tokio::test]
    async fn test() -> DFResult<()> {
        let config = SessionConfig::new().with_batch_size(1);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(AggregateUDF::from(BoolAndAggUDAF::new()));
        ctx.sql(
            r#"create table test_boolean_agg
(
    id integer,
    c  boolean
) as values (1, true),
    (1, true),
    (2, true),
    (2, false),
    (3, true),
    (3, null),
    (4, false),
    (4, null),
    (5, null),
    (5, null);"#,
        )
        .await?;

        let result = ctx
            .sql(
                r#"select
      id,
      booland_agg(c)
    from test_boolean_agg group by id order by id;"#,
            )
            .await?
            .collect()
            .await?;

        print_batches(&result)?;

        Ok(())
    }
}
