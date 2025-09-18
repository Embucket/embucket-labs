use super::errors;
use crate::aggregate::macros::make_udaf_function;
use ahash::RandomState;
use datafusion::arrow::array::{Array, ArrayRef, as_list_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::error::Result as DFResult;
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::cast::{as_string_array, as_uint64_array};
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use serde_json::{Number, Value};
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

// array_union_agg function

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ArrayUnionAggUDAF {
    signature: Signature,
}

impl Default for ArrayUnionAggUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayUnionAggUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl AggregateUDFImpl for ArrayUnionAggUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_union_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayUniqueAggAccumulator::new()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        let values = Arc::new(Field::new_list(
            format_state_name(args.name, "values"),
            Field::new_list_field(DataType::Utf8, true),
            false,
        ));

        let dt = Arc::new(Field::new(
            format_state_name(args.name, "data_type"),
            DataType::UInt64,
            true,
        ));
        Ok(vec![values, dt])
    }
}

#[derive(Debug, Clone)]
enum DType {
    Boolean = 0,
    Float64 = 1,
    Utf8 = 2,
    SemiStructured = 3,
}

#[derive(Debug)]
struct ArrayUniqueAggAccumulator {
    values: Vec<ScalarValue>,
    hash: HashSet<ScalarValue, RandomState>,
    data_type: Option<DType>,
}

impl ArrayUniqueAggAccumulator {
    fn new() -> Self {
        Self {
            values: vec![],
            hash: HashSet::default(),
            data_type: None,
        }
    }
}

impl Accumulator for ArrayUniqueAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        let arr = as_string_array(arr)?;
        let mut buf = Vec::with_capacity(arr.len());
        for v in arr.into_iter().flatten() {
            let json: Value =
                serde_json::from_str(v).context(errors::FailedToSerializeValueSnafu)?;

            if let Value::Array(v) = json {
                buf.clear();
                for v in v {
                    // try to inherit type
                    let s = match v {
                        Value::Null => continue,
                        Value::Bool(_) => {
                            self.data_type = Some(DType::Boolean);
                            v.to_string()
                        }
                        Value::Number(_) => {
                            self.data_type = Some(DType::Float64);
                            v.to_string()
                        }
                        Value::String(_) => {
                            self.data_type = Some(DType::Utf8);
                            v.to_string()
                        }
                        Value::Array(_) | Value::Object(_) => {
                            self.data_type = Some(DType::SemiStructured);
                            v.to_string()
                        }
                    };

                    let sv = ScalarValue::Utf8(Some(s));
                    if !self.hash.contains(&sv) {
                        buf.push(sv);
                    }
                }

                for v in &buf {
                    self.values.push(v.to_owned());
                    self.hash.insert(v.to_owned());
                }
            } else {
                return errors::ArrayUnionAggOnlySupportsJsonArraySnafu.fail()?;
            }
        }

        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cast_precision_loss,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let arr = match &self.data_type {
            None => self
                .values
                .iter()
                .map(|v| {
                    if let ScalarValue::Utf8(v) = v {
                        if let Some(v) = v {
                            Ok(Value::String(v.to_owned()))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        errors::StateValuesShouldBeStringTypeSnafu.fail()?
                    }
                })
                .collect::<DFResult<Vec<_>>>()?,
            Some(dt) => match dt {
                DType::Boolean => self
                    .values
                    .iter()
                    .map(|v| {
                        if let ScalarValue::Utf8(v) = v {
                            if let Some(v) = v {
                                Ok(Value::Bool(
                                    v.parse::<bool>()
                                        .context(errors::FailedToParseBooleanSnafu)?,
                                ))
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            errors::StateValuesShouldBeStringTypeSnafu.fail()?
                        }
                    })
                    .collect::<DFResult<Vec<_>>>()?,
                DType::Float64 => {
                    self.values
                        .iter()
                        .map(|v| {
                            if let ScalarValue::Utf8(v) = v {
                                if let Some(v) = v {
                                    let vv = v
                                        .parse::<f64>()
                                        .context(errors::FailedToParseFloatSnafu)?;
                                    if vv.fract() == 0.0 {
                                        Ok(Value::Number(Number::from(vv as i64)))
                                    } else {
                                        Ok(Value::Number(
                                            // here we loose context error when returning result as Option
                                            Number::from_f64(
                                                v.parse::<f64>()
                                                    .context(errors::FailedToParseFloatSnafu)?,
                                            )
                                            .ok_or_else(|| {
                                                errors::FailedToParseFloatNoSourceSnafu.build()
                                            })?,
                                        ))
                                    }
                                } else {
                                    Ok(Value::Null)
                                }
                            } else {
                                errors::StateValuesShouldBeStringTypeSnafu.fail()?
                            }
                        })
                        .collect::<DFResult<Vec<_>>>()?
                }
                DType::Utf8 => self
                    .values
                    .iter()
                    .map(|v| {
                        if let ScalarValue::Utf8(v) = v {
                            if let Some(v) = v {
                                Ok(Value::String(v.to_owned()))
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            errors::StateValuesShouldBeStringTypeSnafu.fail()?
                        }
                    })
                    .collect::<DFResult<Vec<_>>>()?,
                DType::SemiStructured => self
                    .values
                    .iter()
                    .map(|v| {
                        if let ScalarValue::Utf8(v) = v {
                            if let Some(v) = v {
                                Ok(serde_json::from_str(v)
                                    .context(errors::FailedToSerializeValueSnafu)?)
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            errors::StateValuesShouldBeStringTypeSnafu.fail()?
                        }
                    })
                    .collect::<DFResult<Vec<_>>>()?,
            },
        };

        Ok(ScalarValue::Utf8(Some(
            serde_json::to_string(&arr).context(errors::FailedToSerializeValueSnafu)?,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self) + ScalarValue::size_of_vec(&self.values) - size_of_val(&self.values)
            + ScalarValue::size_of_hashset(&self.hash)
            - size_of_val(&self.hash)
    }

    #[allow(clippy::as_conversions)]
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let values = ScalarValue::new_list(&self.values, &DataType::Utf8, true);
        let dt = ScalarValue::UInt64(self.data_type.clone().map(|dt| dt as u64));
        Ok(vec![ScalarValue::List(values), dt])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        let dt_arr = as_uint64_array(&states[1])?;
        if !dt_arr.is_null(0) {
            match dt_arr.value(0) {
                0 => self.data_type = Some(DType::Boolean),
                1 => self.data_type = Some(DType::Float64),
                2 => self.data_type = Some(DType::Utf8),
                3 => self.data_type = Some(DType::SemiStructured),
                _ => {
                    return errors::ArrayUnionAggOnlySupportsBooleanFloat64AndUtf8Snafu.fail()?;
                }
            }
        }

        let arr: ArrayRef = Arc::new(as_list_array(&states[0]).to_owned().value(0));
        let values = array_to_scalar_vec(&arr)?;
        let mut buf = Vec::with_capacity(values.len());
        for value in values {
            if !self.hash.contains(&value) {
                buf.push(value);
            }
        }

        for v in &buf {
            self.values.push(v.to_owned());
            self.hash.insert(v.to_owned());
        }

        Ok(())
    }
}

fn array_to_scalar_vec(arr: &ArrayRef) -> DFResult<Vec<ScalarValue>> {
    (0..arr.len())
        .map(|i| ScalarValue::try_from_array(arr, i))
        .collect::<DFResult<Vec<_>>>()
}

make_udaf_function!(ArrayUnionAggUDAF);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::AggregateUDF;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_sql() -> DFResult<()> {
        let config = SessionConfig::new()
            .with_batch_size(2)
            .with_coalesce_batches(false)
            .with_enforce_batch_size_in_joins(false);
        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUnionAggUDAF::new()));

        ctx.sql("CREATE TABLE union_test(a string);").await?;
        ctx.sql("INSERT INTO union_test SELECT '[ 1, 1, 2]' UNION ALL SELECT '[ 1, 2, 3]';")
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNION_AGG(a) as distinct_values FROM union_test;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+-----------------+",
                "| distinct_values |",
                "+-----------------+",
                "| [1,1,2,3]       |",
                "+-----------------+",
            ],
            &result
        );

        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUnionAggUDAF::new()));
        ctx.sql("CREATE TABLE union_test(a string);").await?;
        ctx.sql("INSERT INTO union_test SELECT '[ 1, 1.1, 2]' UNION ALL SELECT '[ 1, 2, 3]';")
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNION_AGG(a) as distinct_values FROM union_test;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+-----------------+",
                "| distinct_values |",
                "+-----------------+",
                "| [1,1.1,2,3]     |",
                "+-----------------+",
            ],
            &result
        );

        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUnionAggUDAF::new()));
        ctx.sql("CREATE TABLE union_test(a string);").await?;
        ctx.sql(r#"INSERT INTO union_test SELECT '[ "a", "a", "b"]' UNION ALL SELECT '["a", "b", "c"]';"#)
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNION_AGG(a) as distinct_values FROM union_test;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+-----------------------------------+",
                "| distinct_values                   |",
                "+-----------------------------------+",
                "| [\"\\\"a\\\"\",\"\\\"a\\\"\",\"\\\"b\\\"\",\"\\\"c\\\"\"] |",
                "+-----------------------------------+",
            ],
            &result
        );

        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUnionAggUDAF::new()));
        ctx.sql("CREATE TABLE union_test(a string);").await?;
        ctx.sql(
            "INSERT INTO union_test SELECT '[ [1], [1], [2]]' UNION ALL SELECT '[[1], [2], [3]]';",
        )
        .await?
        .collect()
        .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNION_AGG(a) as distinct_values FROM union_test;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+-------------------+",
                "| distinct_values   |",
                "+-------------------+",
                "| [[1],[1],[2],[3]] |",
                "+-------------------+",
            ],
            &result
        );

        let ctx = SessionContext::new_with_config(config.clone());
        ctx.register_udaf(AggregateUDF::from(ArrayUnionAggUDAF::new()));
        ctx.sql("CREATE TABLE union_test(a string);").await?;
        ctx.sql(r#"INSERT INTO union_test SELECT '[ {"a":1}, {"a":1}, {"a":2}]' UNION ALL SELECT '[{"a":1}, {"a":2}, {"a":3}]';"#)
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT ARRAY_UNION_AGG(a) as distinct_values FROM union_test;")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+-----------------------------------+",
                "| distinct_values                   |",
                "+-----------------------------------+",
                "| [{\"a\":1},{\"a\":1},{\"a\":2},{\"a\":3}] |",
                "+-----------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
