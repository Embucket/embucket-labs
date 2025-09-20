use crate::aggregate::macros::make_udaf_function;
use crate::json::encode_array;
use ahash::RandomState;
use datafusion::arrow::array::{Array, ArrayRef, as_list_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::error::Result as DFResult;
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

// array_unique_agg function
// Returns an ARRAY that contains all of the distinct values from the specified column.
// Syntax: ARRAY_UNIQUE_AGG( <column> )
// Arguments:
// - <column>
//   The column containing the values.
//
// Returns:
// The function returns an ARRAY of distinct values from the specified column. The elements in the
// ARRAY are unordered, and their order is not deterministic.
//
// NULL values in the column are ignored. If the column contains only NULL values or if the table
// is empty, the function returns an empty ARRAY.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ArrayUniqueAggUDAF {
    signature: Signature,
}

impl Default for ArrayUniqueAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayUniqueAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl AggregateUDFImpl for ArrayUniqueAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_unique_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayUniqueAggAccumulator::new(
            acc_args.exprs[0].data_type(acc_args.schema)?,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        let input_dt = args.input_fields[0].data_type().clone();
        let values = Arc::new(Field::new_list(
            format_state_name(args.name, "values"),
            Field::new_list_field(input_dt, true),
            false,
        ));

        Ok(vec![values])
    }
}

#[derive(Debug)]
struct ArrayUniqueAggAccumulator {
    values: Vec<ScalarValue>,
    hash: HashSet<ScalarValue, RandomState>,
    data_type: DataType,
}

impl ArrayUniqueAggAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            values: vec![],
            hash: HashSet::default(),
            data_type,
        }
    }
}

impl Accumulator for ArrayUniqueAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        let scalars = array_to_scalar_vec(arr)?;
        for value in scalars {
            if !self.hash.contains(&value) && !value.is_null() {
                self.values.push(value.clone());
                self.hash.insert(value);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let arr = ScalarValue::iter_to_array(self.values.clone())?;
        let res = encode_array(arr)?;
        Ok(ScalarValue::Utf8(Some(res.to_string())))
    }

    fn size(&self) -> usize {
        size_of_val(self) + ScalarValue::size_of_vec(&self.values) - size_of_val(&self.values)
            + ScalarValue::size_of_hashset(&self.hash)
            - size_of_val(&self.hash)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let values = ScalarValue::new_list(&self.values, &self.data_type, true);
        Ok(vec![ScalarValue::List(values)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let arr: ArrayRef = Arc::new(as_list_array(&states[0]).to_owned().value(0));
        let values = array_to_scalar_vec(&arr)?;
        for value in values {
            if !self.hash.contains(&value) && !value.is_null() {
                self.values.push(value.clone());
                self.hash.insert(value);
            }
        }

        Ok(())
    }
}

fn array_to_scalar_vec(arr: &ArrayRef) -> DFResult<Vec<ScalarValue>> {
    (0..arr.len())
        .map(|i| ScalarValue::try_from_array(arr, i))
        .collect::<DFResult<Vec<_>>>()
}

make_udaf_function!(ArrayUniqueAggUDAF);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::AggregateUDF;

    #[tokio::test]
    async fn test_sql() -> DFResult<()> {
        let config = SessionConfig::new()
            .with_batch_size(1)
            .with_coalesce_batches(false)
            .with_enforce_batch_size_in_joins(false);
        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUniqueAggUDAF::new()));

        ctx.sql("CREATE OR REPLACE TABLE array_unique_agg_test (a INTEGER)")
            .await?;
        ctx.sql("INSERT INTO array_unique_agg_test VALUES (5), (2), (1), (2), (1), (null);")
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNIQUE_AGG(a) AS distinct_values FROM array_unique_agg_test;")
            .await?
            .collect()
            .await?;

        // Check content irrespective of order: must contain unique {5,2,1}
        use datafusion::arrow::array::AsArray;
        use std::collections::HashSet;
        let batch = &result[0];
        let col = batch.column(0).as_string::<i32>();
        let json_str = col.value(0);
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert!(parsed.is_array());
        let arr = parsed.as_array().unwrap();
        let mut set: HashSet<i64> = HashSet::new();
        for v in arr {
            assert!(v.is_number());
            set.insert(v.as_i64().unwrap());
        }
        assert_eq!(arr.len(), set.len());
        assert_eq!(set, HashSet::from_iter([5_i64, 2, 1]));

        let config = SessionConfig::new()
            .with_batch_size(1)
            .with_coalesce_batches(false)
            .with_enforce_batch_size_in_joins(false);
        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUniqueAggUDAF::new()));

        ctx.sql("CREATE OR REPLACE TABLE array_unique_agg_test (a ARRAY<INTEGER>)")
            .await?;
        ctx.sql(
            "INSERT INTO array_unique_agg_test VALUES ([1]), ([2]), ([1]), ([2]), ([1]), ([null]);",
        )
        .await?
        .collect()
        .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNIQUE_AGG(a) AS distinct_values FROM array_unique_agg_test;")
            .await?
            .collect()
            .await?;

        // Validate uniqueness and membership ignoring order for arrays input
        let batch = &result[0];
        let col = batch.column(0).as_string::<i32>();
        let json_str = col.value(0);
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert!(parsed.is_array());
        let arr = parsed.as_array().unwrap();
        // Expect three elements: [1], [2], [null] in any order
        assert_eq!(arr.len(), 3);
        let mut seen_one = false;
        let mut seen_two = false;
        let mut seen_null = false;
        for v in arr {
            assert!(v.is_array());
            let inner = v.as_array().unwrap();
            assert_eq!(inner.len(), 1);
            if inner[0].is_null() {
                seen_null = true;
            } else if inner[0] == serde_json::json!(1) {
                seen_one = true;
            } else if inner[0] == serde_json::json!(2) {
                seen_two = true;
            } else {
                panic!("unexpected element: {:?}", v);
            }
        }
        assert!(seen_one && seen_two && seen_null);

        Ok(())
    }
}
