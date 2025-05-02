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

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion::common::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Accumulator;
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use std::any::Any;

/// Boolxor function
/// Returns TRUE if exactly one Boolean record in the group evaluates to TRUE.
///
/// If all records in the group are NULL, or if the group is empty, the function returns NULL.
///
/// Syntax: `boolxor_agg(<expr>)`

#[derive(Debug, Clone)]
pub struct BoolXorAggUDAF {
    signature: Signature,
}

impl Default for BoolXorAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolXorAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BoolXorAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "boolxor_agg"
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

        let arr = &values[0];
        let mut is_null = true;
        match arr.data_type() {
            DataType::Boolean => {
                let barr = downcast_value!(arr, BooleanArray);
                for val in barr {
                    if val.is_some() {
                        is_null = false;
                    }
                    if matches!(val, Some(true)) {
                        if matches!(self.state, Some(true)) {
                            self.state = Some(false);
                            return Ok(());
                        }
                        self.state = Some(true);
                    }
                }
            }
            _ => {
                unimplemented!()
            }
        }

        if !is_null && !matches!(self.state, Some(true)) {
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

        let mut is_null = true;
        for state in states {
            let v = ScalarValue::try_from_array(state, 0)?;
            if !v.is_null() {
                is_null = false;
            }
            if matches!(v, ScalarValue::Boolean(Some(true))) {
                if matches!(self.state, Some(true)) {
                    self.state = Some(false);
                    return Ok(());
                }

                self.state = Some(true);
            }
        }

        if !is_null && !matches!(self.state, Some(true)) {
            self.state = Some(false);
        }

        Ok(())
    }
}
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
        assert_eq!(acc.state, Some(false));

        let mut acc = BoolAndAggAccumulator::new();
        acc.merge_batch(&[
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
        ])?;
        assert_eq!(acc.state, Some(true));

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
        ctx.register_udaf(AggregateUDF::from(BoolXorAggUDAF::new()));
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
            .sql("select id, boolxor_agg(c) from test_boolean_agg group by id order by id;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+----+---------------------------------+",
                "| id | boolxor_agg(test_boolean_agg.c) |",
                "+----+---------------------------------+",
                "| 1  | false                           |",
                "| 2  | true                            |",
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
