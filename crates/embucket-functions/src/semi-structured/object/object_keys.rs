use crate::semi_structured::errors;
use datafusion::arrow::array::{ArrayRef, StringBuilder, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

// function object_keys
// Returns an array containing the list of keys in the top-most level of the input object.
// Syntax: OBJECT_KEYS(<object>)
// Arguments
// - <object>
// The input must be a JSON object.
// Returns an ARRAY containing the keys.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObjectKeysFunc {
    signature: Signature,
}

impl Default for ObjectKeysFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectKeysFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ObjectKeysFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "object_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let arr = as_string_array(&arr);
        let mut res = StringBuilder::new();
        for v in arr {
            let Some(v) = v else {
                res.append_null();
                continue;
            };

            let json =
                serde_json::from_str::<Value>(v).context(errors::FailedToDeserializeJsonSnafu)?;

            if let Value::Object(map) = json {
                let mut keys = vec![];
                for key in map.keys().cloned() {
                    keys.push(Value::String(key));
                }

                let v = serde_json::to_string(&Value::Array(keys))
                    .context(errors::FailedToSerializeValueSnafu)?;
                res.append_value(v);
            } else {
                return errors::InputMustBeJsonObjectSnafu.fail()?;
            }
        }

        let res: ArrayRef = Arc::new(res.finish());

        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(res)
        })
    }
}

crate::macros::make_udf_function!(ObjectKeysFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use datafusion_common::assert_batches_eq;

    #[tokio::test]
    async fn test_object_keys() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ObjectKeysFunc::new().into());

        let sql = "SELECT object_keys('{\"a\": 1, \"b\": 2, \"c\": 3}') AS keys";
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------+",
                "| keys          |",
                "+---------------+",
                "| [\"a\",\"b\",\"c\"] |",
                "+---------------+",
            ],
            &results
        );

        Ok(())
    }
}
