use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, json};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraysToObjectUDF {
    signature: Signature,
}

impl ArraysToObjectUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn create_object(keys: &[Option<String>], values: &[Value]) -> Option<String> {
        if keys.len() != values.len() {
            return None;
        }

        let mut obj = serde_json::Map::new();

        for (key_opt, value) in keys.iter().zip(values.iter()) {
            if let Some(key) = key_opt {
                obj.insert(key.clone(), value.clone());
            }
        }

        Some(json!(obj).to_string())
    }
}

impl Default for ArraysToObjectUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraysToObjectUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "arrays_to_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let keys_arg = args
            .first()
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "keys array" }.build())?;
        let values_arg = args.get(1).ok_or_else(|| {
            errors::ExpectedNamedArgumentSnafu {
                name: "values array",
            }
            .build()
        })?;

        match (keys_arg, values_arg) {
            (ColumnarValue::Array(keys_array), ColumnarValue::Array(values_array)) => {
                let keys_string_array = keys_array.as_string::<i32>();
                let values_string_array = values_array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..keys_string_array.len() {
                    if keys_string_array.is_null(i) && values_string_array.is_null(i) {
                        results.push(None);
                        continue;
                    }

                    let keys: Vec<Option<String>> = (0..keys_string_array.len())
                        .map(|j| {
                            if keys_string_array.is_null(j) {
                                None
                            } else {
                                Some(keys_string_array.value(j).to_string())
                            }
                        })
                        .collect();

                    let values: Vec<Value> = (0..values_string_array.len())
                        .map(|j| {
                            if values_string_array.is_null(j) {
                                Value::Null
                            } else {
                                // Try to parse as JSON, fallback to string if not valid JSON
                                match serde_json::from_str(values_string_array.value(j)) {
                                    Ok(val) => val,
                                    Err(_) => {
                                        Value::String(values_string_array.value(j).to_string())
                                    }
                                }
                            }
                        })
                        .collect();

                    results.push(Self::create_object(&keys, &values));
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(keys_value), ColumnarValue::Scalar(values_value)) => {
                // Handle NULL inputs
                if keys_value.is_null() || values_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let ScalarValue::Utf8(Some(keys_str)) = keys_value else {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                };

                let ScalarValue::Utf8(Some(values_str)) = values_value else {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                };

                // Parse arrays
                let keys: Value = serde_json::from_str(keys_str)
                    .context(errors::FailedToDeserializeJsonEntitySnafu { entity: "keys" })?;

                let values: Value = serde_json::from_str(values_str)
                    .context(errors::FailedToDeserializeJsonEntitySnafu { entity: "values" })?;

                if let (Value::Array(key_array), Value::Array(value_array)) = (keys, values) {
                    let keys: Vec<Option<String>> = key_array
                        .into_iter()
                        .map(|v| match v {
                            Value::String(s) => Some(s),
                            Value::Null => None,
                            _ => Some(v.to_string()),
                        })
                        .collect();

                    let result = Self::create_object(&keys, &value_array);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                } else {
                    errors::ArgumentsMustBeJsonArraysSnafu.fail()?
                }
            }
            _ => errors::ArgumentsMustBeJsonArraysSnafu.fail()?,
        }
    }
}

make_udf_function!(ArraysToObjectUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_arrays_to_object() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArraysToObjectUDF::new()));

        // Test basic key-value mapping
        let sql = "SELECT arrays_to_object(array_construct('key1', 'key2', 'key3'), array_construct(1, 2, 3)) as obj";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------------------------+",
                "| obj                          |",
                "+------------------------------+",
                "| {\"key1\":1,\"key2\":2,\"key3\":3} |",
                "+------------------------------+",
            ],
            &result
        );

        // Test with NULL key
        let sql = "SELECT arrays_to_object(array_construct('key1', NULL, 'key3'), array_construct(1, 2, 3)) as obj_null_key";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+",
                "| obj_null_key        |",
                "+---------------------+",
                "| {\"key1\":1,\"key3\":3} |",
                "+---------------------+",
            ],
            &result
        );

        // Test with NULL value
        let sql = "SELECT arrays_to_object(array_construct('key1', 'key2', 'key3'), array_construct(1, NULL, 3)) as obj_null_value";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------------------+",
                "| obj_null_value                  |",
                "+---------------------------------+",
                "| {\"key1\":1,\"key2\":null,\"key3\":3} |",
                "+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
