use crate::aggregate::macros::make_udaf_function;
use datafusion::arrow::array::{Array, ArrayRef, as_string_array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use std::any::Any;
use std::collections::HashSet;
use std::mem::size_of_val;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ListAggUDAF {
    signature: Signature,
}

impl Default for ListAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl ListAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ListAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "listagg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.exprs.is_empty() || acc_args.exprs.len() > 2 {
            return exec_err!(
                "LISTAGG requires 1 or 2 arguments, got {}",
                acc_args.exprs.len()
            );
        }
        Ok(Box::new(ListAggAccumulator::new(acc_args.is_distinct)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "agg"),
                DataType::Utf8,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "delimiter"),
                DataType::Utf8,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "seen_values"),
                DataType::Utf8,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct ListAggAccumulator {
    result: Option<String>,
    delimiter: String,
    delimiter_set: bool,
    is_distinct: bool,
    seen_values: HashSet<String>,
}

impl ListAggAccumulator {
    fn new(is_distinct: bool) -> Self {
        Self {
            result: None,
            delimiter: String::new(),
            delimiter_set: false,
            is_distinct,
            seen_values: HashSet::new(),
        }
    }

    fn append_value(&mut self, val: &str) {
        // If DISTINCT is enabled, check if we've seen this value before
        if self.is_distinct && !self.seen_values.insert(val.to_string()) {
            // Value already exists, skip it
            return;
        }

        match &mut self.result {
            Some(res) => {
                if !res.is_empty() {
                    res.push_str(&self.delimiter);
                }
                res.push_str(val);
            }
            None => {
                self.result = Some(val.to_string());
            }
        }
    }
}

impl Accumulator for ListAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];

        // Handle delimiter if present
        if values.len() > 1 && !self.delimiter_set {
            if values[1].data_type() == &DataType::Null {
                // Delimiter is NULL, keep default empty string
            } else {
                // Convert delimiter to string
                let delim_string_arr = compute::cast(&values[1], &DataType::Utf8)?;
                let delim_arr = as_string_array(&delim_string_arr);
                for i in 0..delim_arr.len() {
                    if !delim_arr.is_null(i) {
                        self.delimiter = delim_arr.value(i).to_string();
                        self.delimiter_set = true;
                        break;
                    }
                }
            }
        }

        // Handle the main array - convert any type to string
        if arr.data_type() == &DataType::Null {
            // All values are NULL - do nothing, will return empty string as per Snowflake spec
        } else {
            // Convert the array to string representation
            let string_arr = compute::cast(arr, &DataType::Utf8)?;
            let string_arr = as_string_array(&string_arr);

            for i in 0..string_arr.len() {
                if string_arr.is_null(i) {
                    continue;
                }
                self.append_value(string_arr.value(i));
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        // Convert aggregated state to string if needed
        let agg_string_arr = compute::cast(&states[0], &DataType::Utf8)?;
        let agg_arr = as_string_array(&agg_string_arr);

        if states.len() > 1 && !self.delimiter_set {
            let delim_string_arr = compute::cast(&states[1], &DataType::Utf8)?;
            let delim_arr = as_string_array(&delim_string_arr);
            if delim_arr.len() > 0 && !delim_arr.is_null(0) {
                self.delimiter = delim_arr.value(0).to_string();
                self.delimiter_set = true;
            }
        }

        // Merge seen values if DISTINCT is enabled
        if self.is_distinct && states.len() > 2 {
            let seen_string_arr = compute::cast(&states[2], &DataType::Utf8)?;
            let seen_arr = as_string_array(&seen_string_arr);
            for i in 0..seen_arr.len() {
                if !seen_arr.is_null(i) {
                    let seen_values_str = seen_arr.value(i);
                    if !seen_values_str.is_empty() {
                        for value in seen_values_str.split('\0') {
                            self.seen_values.insert(value.to_string());
                        }
                    }
                }
            }
        }

        for i in 0..agg_arr.len() {
            if agg_arr.is_null(i) {
                continue;
            }
            self.append_value(agg_arr.value(i));
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let seen_values_str = if self.is_distinct {
            Some(
                self.seen_values
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\0"),
            )
        } else {
            None
        };

        Ok(vec![
            ScalarValue::Utf8(self.result.clone()),
            ScalarValue::Utf8(Some(self.delimiter.clone())),
            ScalarValue::Utf8(seen_values_str),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(Some(
            self.result.clone().unwrap_or_default(),
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.result.as_ref().map_or(0, std::string::String::len)
            + self.delimiter.len()
            + self
                .seen_values
                .iter()
                .map(std::string::String::len)
                .sum::<usize>()
    }
}

make_udaf_function!(ListAggUDAF);
