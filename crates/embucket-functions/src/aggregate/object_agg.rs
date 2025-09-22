use super::errors as agg_errors;
use crate::macros::make_udaf_function;
use serde_json::Value as JsonValue;
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::array::as_list_array;
use datafusion::arrow::array::{Array, new_empty_array};
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::ScalarValue;

use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::Volatility;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ObjectAggUDAF {
    signature: Signature,
}

impl Default for ObjectAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectAggUDAF {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ObjectAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "object_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // one field: a List<Struct<key,value>>
        let value_dt = args.input_fields[1].data_type().clone();
        let entry_struct = Arc::new(Field::new_list(
            format_state_name(args.name, "object_agg"),
            Field::new_list_field(
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value_dt, true),
                ])),
                true,
            ),
            true,
        ));
        Ok(vec![entry_struct])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return exec_err!("OBJECT_AGG does not yet support DISTINCT");
        }

        if !acc_args.order_bys.is_empty() {
            return exec_err!("OBJECT_AGG does not yet support ORDER BY");
        }

        let key_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        match key_type {
            DataType::Utf8 | DataType::LargeUtf8 => {}
            _ => {
                return exec_err!("OBJECT_AGG key must be Utf8 or LargeUtf8, got {key_type:?}");
            }
        }

        let value_type = acc_args.exprs[1].data_type(acc_args.schema)?;

        Ok(Box::new(ObjectAggAccumulator::try_new(&value_type)))
    }
}

#[derive(Debug)]
struct ObjectAggAccumulator {
    keys: Vec<ScalarValue>,
    values: Vec<ScalarValue>,
    value_type: DataType,
    keys_seen: HashSet<ScalarValue>,
}

impl ObjectAggAccumulator {
    pub fn try_new(value_type: &DataType) -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            value_type: value_type.clone(),
            keys_seen: HashSet::new(),
        }
    }
}

impl Accumulator for ObjectAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(key_array) = values[0].as_any().downcast_ref::<StringArray>() else {
            return internal_err!("Key column must be Utf8 or LargeUtf8");
        };

        let val_array = &values[1];

        for row in 0..key_array.len() {
            if key_array.is_null(row) || val_array.is_null(row) {
                continue;
            }

            let key_str = ScalarValue::from(key_array.value(row));
            if !self.keys_seen.insert(key_str.clone()) {
                return internal_err!("Duplicate keys are not allowed, key found: {}", key_str);
            }

            let k = ScalarValue::Utf8(Some(key_array.value(row).to_string()));
            let v = ScalarValue::try_from_array(val_array, row)?;
            self.keys.push(k);
            self.values.push(v);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        // states[0] is the ListArray<Struct<key,value>> from your state()
        let list = as_list_array(&*states[0]);

        for maybe_struct in list.iter().flatten() {
            let Some(array) = maybe_struct.as_any().downcast_ref::<StructArray>() else {
                return internal_err!("OBJECT_AGG state is not a StructArray");
            };

            let key_arr = array
                .column_by_name("key")
                .ok_or_else(|| agg_errors::MissingKeyColumnSnafu.build())?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| agg_errors::KeyColumnShouldBeStringTypeSnafu.build())?;

            let val_arr = array
                .column_by_name("value")
                .ok_or_else(|| agg_errors::MissingValueColumnSnafu.build())?;

            for i in 0..array.len() {
                if array.is_null(i) {
                    continue;
                }
                let key = ScalarValue::Utf8(Some(key_arr.value(i).to_string()));
                if !self.keys_seen.insert(key.clone()) {
                    return internal_err!(
                        "Duplicate keys are not allowed, key found: {}",
                        key_arr.value(i)
                    );
                }
                let val = ScalarValue::try_from_array(val_arr, i)?;
                self.keys.push(key);
                self.values.push(val);
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let key_array: ArrayRef = if self.keys.is_empty() {
            new_empty_array(&DataType::Utf8)
        } else {
            ScalarValue::iter_to_array(self.keys.clone())?
        };

        let val_array: ArrayRef = if self.values.is_empty() {
            new_empty_array(&self.value_type)
        } else {
            ScalarValue::iter_to_array(self.values.clone())?
        };

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let val_field = Arc::new(Field::new("value", self.value_type.clone(), true));

        let entries = StructArray::from(vec![(key_field, key_array), (val_field, val_array)]);

        let map_scalar = SingleRowListArrayBuilder::new(Arc::new(entries)).build_list_scalar();

        Ok(vec![map_scalar])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut obj = serde_json::Map::with_capacity(self.keys.len());
        for (k_sv, v_sv) in self.keys.iter().zip(self.values.iter()) {
            let key = match k_sv {
                ScalarValue::Utf8(Some(s)) => s.clone(),
                _ => continue,
            };

            let value: JsonValue = match v_sv {
                ScalarValue::Utf8(Some(s)) => {
                    serde_json::from_str(s).context(agg_errors::FailedToSerializeValueSnafu)?
                }
                _ => continue,
            };
            obj.insert(key, value);
        }

        let json_text = JsonValue::Object(obj).to_string();

        Ok(ScalarValue::Utf8(Some(json_text)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + ScalarValue::size_of_vec(&self.keys) - size_of_val(&self.keys)
            + ScalarValue::size_of_vec(&self.values)
            - size_of_val(&self.values)
            + self.value_type.size()
            - size_of_val(&self.value_type)
            + ScalarValue::size_of_hashset(&self.keys_seen)
            - size_of_val(&self.keys_seen)
    }
}

make_udaf_function!(ObjectAggUDAF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};

    use datafusion_common::{Result, internal_err};
    use datafusion_physical_plan::Accumulator;
    use datafusion_physical_plan::expressions::Column;
    use serde_json::json;
    use serde_json::{Map as JsonMap, Value as JsonValue};
    use std::sync::Arc;

    struct ObjectAggAccumulatorBuilder {
        data_type: DataType,
        distinct: bool,
        schema: Schema,
    }

    impl ObjectAggAccumulatorBuilder {
        fn string() -> Self {
            Self::new(DataType::Utf8)
        }

        fn new(value_type: DataType) -> Self {
            let schema = Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", value_type.clone(), true),
            ]);

            Self {
                data_type: value_type,
                distinct: Default::default(),
                schema,
            }
        }

        fn build(&self) -> Result<Box<dyn Accumulator>> {
            let return_field: FieldRef =
                Arc::new(Field::new("result", self.data_type.clone(), true));
            ObjectAggUDAF::default().accumulator(AccumulatorArgs {
                return_field,
                schema: &self.schema,
                ignore_nulls: false,
                order_bys: &[],
                is_reversed: false,
                name: "",
                is_distinct: self.distinct,
                exprs: &[
                    Arc::new(Column::new("key", 0)),
                    Arc::new(Column::new("value", 1)),
                ],
            })
        }

        fn build_two(&self) -> Result<(Box<dyn Accumulator>, Box<dyn Accumulator>)> {
            Ok((self.build()?, self.build()?))
        }
    }

    fn data<T, const N: usize>(list: [T; N]) -> ArrayRef
    where
        ScalarValue: From<T>,
    {
        let values: Vec<_> = list.into_iter().map(ScalarValue::from).collect();
        let array: ArrayRef = if values.is_empty() {
            new_empty_array(&DataType::Utf8)
        } else {
            ScalarValue::iter_to_array(values).unwrap()
        };
        array
    }

    fn merge(
        mut acc1: Box<dyn Accumulator>,
        mut acc2: Box<dyn Accumulator>,
    ) -> Result<Box<dyn Accumulator>> {
        let intermediate_state = acc2.state().and_then(|e| {
            e.iter()
                .map(ScalarValue::to_array)
                .collect::<Result<Vec<ArrayRef>>>()
        })?;
        acc1.merge_batch(&intermediate_state)?;
        Ok(acc1)
    }

    #[test]
    fn basic_object_agg() -> Result<()> {
        let (mut acc1, mut acc2) = ObjectAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"]), data(["1", "2", "3"])])?;

        acc2.update_batch(&[data(["d", "e"]), data(["4", "5"])])?;

        acc1 = merge(acc1, acc2)?;
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected Utf8 JSON");
        };

        // parse and verify the JSON object
        let map: JsonMap<String, JsonValue> = serde_json::from_str(&json).unwrap();
        assert_eq!(map.len(), 5);
        assert_eq!(map.get("a").unwrap(), &JsonValue::Number(1.into()));
        assert_eq!(map.get("b").unwrap(), &JsonValue::Number(2.into()));
        assert_eq!(map.get("c").unwrap(), &JsonValue::Number(3.into()));
        assert_eq!(map.get("d").unwrap(), &JsonValue::Number(4.into()));
        assert_eq!(map.get("e").unwrap(), &JsonValue::Number(5.into()));
        Ok(())
    }

    #[test]
    fn duplicate_key_error() -> Result<()> {
        let (mut acc1, mut acc2) = ObjectAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[data(["x", "y"]), data(["1", "2"])])?;
        acc2.update_batch(&[data(["x", "z"]), data(["3", "4"])])?;

        let intermediate = acc2
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;
        let err = acc1.merge_batch(&intermediate).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Duplicate keys"), "got: {msg}");
        Ok(())
    }

    /// Null keys are ignored entirely, non-null keys still aggregate
    #[test]
    fn null_key_ignored() -> Result<()> {
        let (mut acc1, mut acc2) = ObjectAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[data([None, Some("a")]), data([Some("10"), Some("1")])])?;
        acc2.update_batch(&[data(["b"]), data(["2"])])?;

        acc1 = merge(acc1, acc2)?;
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected JSON");
        };
        let map: JsonMap<_, _> = serde_json::from_str(&json).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a").unwrap(), &JsonValue::Number(1.into()));
        assert_eq!(map.get("b").unwrap(), &JsonValue::Number(2.into()));
        Ok(())
    }

    #[test]
    fn null_pairs_are_ignored() -> Result<()> {
        let (mut acc1, acc2) = ObjectAggAccumulatorBuilder::string().build_two()?;

        acc1.update_batch(&[
            data([Some("a"), None, Some("b"), Some("c")]),
            data([Some("1"), Some("2"), None, Some("3")]),
        ])?;

        acc1 = merge(acc1, acc2)?;
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected an Utf8 JSON string");
        };

        let map: JsonMap<String, JsonValue> = serde_json::from_str(&json).unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a").unwrap(), &JsonValue::Number(1.into()));
        assert_eq!(map.get("c").unwrap(), &JsonValue::Number(3.into()));
        Ok(())
    }

    /// Completely empty input should produce an empty object
    #[test]
    fn empty_input_produces_empty_object() -> Result<()> {
        let (mut acc1, mut acc2) = ObjectAggAccumulatorBuilder::string().build_two()?;

        // feed no rows into either accumulator
        acc1.update_batch(&[data::<Option<&str>, 0>([]), data::<Option<&str>, 0>([])])?;
        acc2.update_batch(&[data::<Option<&str>, 0>([]), data::<Option<&str>, 0>([])])?;

        acc1 = merge(acc1, acc2)?;
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected JSON");
        };
        assert_eq!(json, "{}");
        Ok(())
    }

    /// chain three partitions
    #[test]
    fn merge_three_partitions() -> Result<()> {
        let builder = ObjectAggAccumulatorBuilder::string();
        let (mut a, mut b) = builder.build_two()?;
        let mut c = builder.build()?;

        a.update_batch(&[data(["k1"]), data(["1"])])?;
        b.update_batch(&[data(["k2"]), data(["2"])])?;
        c.update_batch(&[data(["k3"]), data(["3"])])?;

        let mut ab = merge(a, b)?;
        ab = merge(ab, c)?;

        let ScalarValue::Utf8(Some(json)) = ab.evaluate()? else {
            return internal_err!("expected JSON");
        };
        let map: JsonMap<_, _> = serde_json::from_str(&json).unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map.get("k1").unwrap(), &JsonValue::Number(1.into()));
        assert_eq!(map.get("k2").unwrap(), &JsonValue::Number(2.into()));
        assert_eq!(map.get("k3").unwrap(), &JsonValue::Number(3.into()));
        Ok(())
    }

    #[test]
    fn raw_json_string_matches_expected() -> Result<()> {
        let (mut acc1, mut acc2) = ObjectAggAccumulatorBuilder::string().build_two()?;

        // feed a couple of key→value pairs in two partitions
        acc1.update_batch(&[data(["a", "b"]), data(["1", "2"])])?;
        acc2.update_batch(&[data(["c"]), data(["3"])])?;

        // merge them
        acc1 = merge(acc1, acc2)?;

        // evaluate to get the raw JSON
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected an Utf8 JSON string");
        };

        assert_eq!(json, r#"{"a":1,"b":2,"c":3}"#);

        Ok(())
    }

    #[test]
    fn object_agg_nested_json_values() -> Result<()> {
        let builder = {
            let mut b = ObjectAggAccumulatorBuilder::string();
            // override schema so we know the second field is Utf8
            b.schema = Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]);
            b
        };
        let (mut acc1, mut acc2) = builder.build_two()?;

        acc1.update_batch(&[data(["a", "b"]), data([r#"{"x":1}"#, r#"["foo","bar"]"#])])?;

        acc2.update_batch(&[data(["c", "d"]), data(["null", "true"])])?;

        acc1 = merge(acc1, acc2)?;
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected Utf8 JSON");
        };

        let map: JsonMap<String, JsonValue> = serde_json::from_str(&json).unwrap();
        assert_eq!(map.len(), 4);

        assert_eq!(map.get("a").unwrap(), &json!({"x":1}));
        assert_eq!(map.get("b").unwrap(), &json!(["foo", "bar"]));
        assert_eq!(map.get("c").unwrap(), &JsonValue::Null);
        assert_eq!(map.get("d").unwrap(), &JsonValue::Bool(true));

        Ok(())
    }

    #[test]
    fn object_agg_json_number_values() -> Result<()> {
        let builder = {
            let mut b = ObjectAggAccumulatorBuilder::string();
            b.schema = Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]);
            b
        };
        let (mut acc1, mut acc2) = builder.build_two()?;

        acc1.update_batch(&[data(["x", "y"]), data(["42", "3.14"])])?;

        // partition 2: z→0
        acc2.update_batch(&[data(["z"]), data(["0"])])?;

        acc1 = merge(acc1, acc2)?;
        let ScalarValue::Utf8(Some(json)) = acc1.evaluate()? else {
            return internal_err!("expected Utf8 JSON");
        };

        assert_eq!(json, r#"{"x":42,"y":3.14,"z":0}"#);

        Ok(())
    }
}
