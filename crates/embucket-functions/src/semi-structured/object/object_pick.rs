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
pub struct ObjectPickUDF {
    signature: Signature,
}

impl ObjectPickUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn pick_keys(object_value: Value, keys: Vec<String>) -> DFResult<Option<String>> {
        // Ensure the first argument is an object
        if let Value::Object(obj) = object_value {
            let mut new_obj = serde_json::Map::new();

            // Only include specified keys that exist in the original object
            for key in keys {
                if let Some(value) = obj.get(&key) {
                    new_obj.insert(key, value.clone());
                }
            }

            // Convert back to JSON string
            Ok(Some(
                to_string(&Value::Object(new_obj)).context(errors::FailedToSerializeResultSnafu)?,
            ))
        } else {
            errors::ArgumentMustBeJsonObjectSnafu { argument: "First" }.fail()?
        }
    }
}

impl Default for ObjectPickUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ObjectPickUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "object_pick"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let object_str = args
            .first()
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "object" }.build())?;

        // Get all keys from remaining arguments
        let mut keys = Vec::new();

        // Check if second argument is an array
        if args.len() == 2 {
            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(array_str))) = &args[1] {
                // Try to parse as JSON array first
                if let Ok(Value::Array(json_array)) = from_str::<Value>(array_str) {
                    for value in json_array {
                        if let Value::String(key) = value {
                            keys.push(key);
                        }
                    }
                } else {
                    // If not a JSON array, treat as a single key
                    keys.push(array_str.clone());
                }
            }
        } else {
            // Handle individual key arguments
            for arg in args.iter().skip(1) {
                if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(key))) = arg {
                    keys.push(key.clone());
                }
            }
        }

        match object_str {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let object_str = string_array.value(i);
                        let object_json: Value =
                            from_str(object_str).context(errors::FailedToDeserializeJsonSnafu)?;
                        results.push(Self::pick_keys(object_json, keys.clone())?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(object_value) => {
                match object_value {
                    ScalarValue::Utf8(Some(object_str)) => {
                        // Parse object string to JSON Value
                        let object_json: Value =
                            from_str(object_str).context(errors::FailedToDeserializeJsonSnafu)?;

                        let result = Self::pick_keys(object_json, keys)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
        }
    }
}

make_udf_function!(ObjectPickUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_object_pick() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDF
        ctx.register_udf(ScalarUDF::from(ObjectPickUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));

        // Test picking specific keys
        let sql = "SELECT object_pick('{\"a\": 1, \"b\": 2, \"c\": 3}', 'a', 'b') as picked";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| picked        |",
                "+---------------+",
                "| {\"a\":1,\"b\":2} |",
                "+---------------+",
            ],
            &result
        );

        // Test picking with array argument
        let sql = "SELECT object_pick('{\"a\": 1, \"b\": 2, \"c\": 3}', array_construct('a', 'b')) as picked2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| picked2       |",
                "+---------------+",
                "| {\"a\":1,\"b\":2} |",
                "+---------------+",
            ],
            &result
        );

        // Test with non-existent keys
        let sql = "SELECT object_pick('{\"a\": 1, \"b\": 2}', 'c') as picked3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| picked3 |",
                "+---------+",
                "| {}      |",
                "+---------+",
            ],
            &result
        );

        // Test with NULL input
        let sql = "SELECT object_pick(NULL, 'a') as null_input";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| null_input |",
                "+------------+",
                "|            |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
