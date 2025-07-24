use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::{StringBuilder, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ParseJsonFunc {
    signature: Signature,
}

impl Default for ParseJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseJsonFunc {
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

impl ScalarUDFImpl for ParseJsonFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_json"
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
                let v = v.replace(",,", ",null,");
                let v = v.replace(",]", ",null]");
                let v = v.replace("[,", "[null,");
                let v = serde_json::from_str::<Value>(&v)
                    .context(errors::FailedToSerializeValueSnafu)?;
                if v.is_null() {
                    b.append_null();
                } else {
                    b.append_value(v.to_string());
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

make_udf_function!(ParseJsonFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new()));

        let sql = "SELECT parse_json('{\"key\": \"value\"}') AS parsed_json";
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

        let sql = "SELECT parse_json('null') AS parsed_json";
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

        let sql = "SELECT parse_json('[ null ]') AS parsed_json";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+-------------+",
                "| parsed_json |",
                "+-------------+",
                "| [null]      |",
                "+-------------+",
            ],
            &result
        );

        let sql = "SELECT parse_json('{\"invalid\": \"json\"') AS parsed_json";
        assert!(ctx.sql(sql).await?.collect().await.is_err());

        let sql = r#"SELECT parse_json('[-1, 12, 289, 2188, false,]') AS parsed_json"#;
        let result = ctx.sql(sql).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+-----------------------------+",
                "| parsed_json                 |",
                "+-----------------------------+",
                "| [-1,12,289,2188,false,null] |",
                "+-----------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
