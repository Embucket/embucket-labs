use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_str, to_string};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraySortUDF {
    signature: Signature,
}

impl ArraySortUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Number(a), Value::Number(b)) => {
                if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
                    a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    std::cmp::Ordering::Equal
                }
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            // Type-based ordering for different types, with nulls always last
            _ => {
                let type_order = |v: &Value| {
                    match v {
                        Value::Null => 5, // Move nulls to end
                        Value::Bool(_) => 0,
                        Value::Number(_) => 1,
                        Value::String(_) => 2,
                        Value::Array(_) => 3,
                        Value::Object(_) => 4,
                    }
                };
                type_order(a).cmp(&type_order(b))
            }
        }
    }

    fn sort_array(
        array_value: Value,
        sort_ascending: bool,
        _nulls_first: bool,
    ) -> DFResult<Option<String>> {
        if let Value::Array(array) = array_value {
            // Convert array elements to a format that can be sorted
            let mut elements: Vec<Option<Value>> = array
                .into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    v => Some(v),
                })
                .collect();

            // Sort the array, putting nulls last for ascending and first for descending
            elements.sort_by(|a, b| {
                match (a, b) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => {
                        if sort_ascending {
                            std::cmp::Ordering::Greater // Nulls last for ascending
                        } else {
                            std::cmp::Ordering::Less // Nulls first for descending
                        }
                    }
                    (Some(_), None) => {
                        if sort_ascending {
                            std::cmp::Ordering::Less // Non-nulls before nulls for ascending
                        } else {
                            std::cmp::Ordering::Greater // Non-nulls after nulls for descending
                        }
                    }
                    (Some(a_val), Some(b_val)) => {
                        let cmp = Self::compare_json_values(a_val, b_val);
                        if sort_ascending { cmp } else { cmp.reverse() }
                    }
                }
            });

            // Convert back to JSON array
            let sorted_array: Vec<Value> = elements
                .into_iter()
                .map(|opt| opt.unwrap_or(Value::Null))
                .collect();

            Ok(Some(
                to_string(&sorted_array).context(errors::FailedToSerializeResultSnafu)?,
            ))
        } else {
            errors::ArgumentMustBeJsonArraySnafu { argument: "First" }.fail()?
        }
    }
}

impl Default for ArraySortUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraySortUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Get array argument
        let array_arg = args
            .first()
            .ok_or_else(|| errors::ArrayArgumentExpectedSnafu.build())?;

        // Get optional sort_ascending argument (default: true)
        let sort_ascending = args.get(1).is_none_or(|v| match v {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => *b,
            _ => true,
        });

        // Get optional nulls_first argument (default: true)
        let nulls_first = args.get(2).is_none_or(|v| match v {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => *b,
            _ => true,
        });

        match array_arg {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_str = string_array.value(i);
                        let array_json: Value =
                            from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                        results.push(Self::sort_array(array_json, sort_ascending, nulls_first)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => match array_value {
                ScalarValue::Utf8(Some(array_str)) => {
                    let array_json: Value =
                        from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                    let result = Self::sort_array(array_json, sort_ascending, nulls_first)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                }
                ScalarValue::Utf8(None) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                _ => errors::ArgumentMustBeJsonArraySnafu { argument: "First" }.fail()?,
            },
        }
    }
}

make_udf_function!(ArraySortUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_sort() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArraySortUDF::new()));

        // Test basic sorting
        let sql = "SELECT array_sort(array_construct(20, NULL, 0, NULL, 10)) as sorted";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| sorted              |",
                "+---------------------+",
                "| [0,10,20,null,null] |",
                "+---------------------+",
            ],
            &result
        );

        // Test descending order
        let sql = "SELECT array_sort(array_construct(20, NULL, 0, NULL, 10), false) as desc_sort";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| desc_sort           |",
                "+---------------------+",
                "| [null,null,20,10,0] |",
                "+---------------------+",
            ],
            &result
        );

        // Test nulls last
        let sql =
            "SELECT array_sort(array_construct(20, NULL, 0, NULL, 10), true, false) as nulls_last";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| nulls_last          |",
                "+---------------------+",
                "| [0,10,20,null,null] |",
                "+---------------------+",
            ],
            &result
        );

        // Test with mixed types
        let sql = "SELECT array_sort(array_construct('a', 'c', 'b')) as str_sort";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| str_sort      |",
                "+---------------+",
                "| [\"a\",\"b\",\"c\"] |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }
}
