use crate::macros::make_udf_function;
use datafusion::arrow::array::{StringBuilder, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct TryParseJsonFunc {
    signature: Signature,
}

impl Default for TryParseJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryParseJsonFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(1), TypeSignature::String(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TryParseJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "try_parse_json"
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

        let mut b = StringBuilder::with_capacity(arr.len(), 1024);
        let input = as_string_array(&arr);

        for v in input {
            if let Some(v) = v {
                match serde_json::from_str::<Value>(v) {
                    Ok(v) => {
                        b.append_value(v.to_string());
                    }
                    Err(_) => b.append_null(),
                }
            } else {
                b.append_null();
            }
        }

        let res = b.finish();
        Ok(if arr.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

make_udf_function!(TryParseJsonFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(TryParseJsonFunc::new()));

        let sql = "SELECT try_parse_json('{\"key\": \"value\"}') AS parsed_json";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+-----------------+",
                "| parsed_json     |",
                "+-----------------+",
                "| {\"key\":\"value\"} |",
                "+-----------------+",
            ],
            &result
        );

        let sql = "SELECT try_parse_json('{\"invalid\": \"json\"') AS parsed_json";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+-------------+",
                "| parsed_json |",
                "+-------------+",
                "|             |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
