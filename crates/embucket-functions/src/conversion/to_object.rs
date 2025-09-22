use super::errors as conv_errors;
use crate::macros::make_udf_function;
use datafusion::arrow::array::{Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_generic_string_array;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::OptionExt;
use std::any::Any;
use std::sync::Arc;

/// `TO_OBJECT` function implementation
///
/// Converts the input expression to an OBJECT value.
///
/// Syntax: `TO_OBJECT(<expr>)`
///
/// Arguments:
/// - `<expr>`: The expression to convert to an object. If the expression is NULL, it returns NULL.
///
/// Returns:
/// This function returns either an OBJECT or NULL:
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToObjectFunc {
    signature: Signature,
}

impl Default for ToObjectFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToObjectFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToObjectFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let array = args
            .first()
            .context(conv_errors::NotEnoughArgumentsSnafu {
                got: 0usize,
                at_least: 1usize,
            })?
            .clone()
            .into_array(number_rows)?;

        let array = match array.data_type() {
            DataType::Null => ScalarValue::Utf8(None).to_array_of_size(array.len())?,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let arr: &StringArray = as_generic_string_array(&array)?;
                let mut result = StringBuilder::with_capacity(arr.len(), 1024);

                for opt in arr {
                    result.append_option(opt.map_or_else(
                        || Ok(None),
                        |str| {
                            match serde_json::from_str::<Value>(str) {
                                Ok(Value::Object(_)) => Ok(Some(str)),
                                Ok(_) => conv_errors::FailedToCastVariantSnafu {
                                    value: str.to_string(),
                                    real_type: "OBJECT".to_string(),
                                }
                                .fail(),
                                Err(_) => conv_errors::InvalidTypeForParameterSnafu {
                                    value: format!("'{str}'"),
                                    parameter: "TO_OBJECT".to_string(),
                                }
                                .fail(),
                            }
                        },
                    )?);
                }

                Arc::new(result.finish())
            }
            other => {
                return conv_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.clone(),
                    position: 1usize,
                }
                .fail()?;
            }
        };

        Ok(ColumnarValue::Array(array))
    }
}

make_udf_function!(ToObjectFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversion::to_object::ToObjectFunc;
    use crate::semi_structured::parse_json::ParseJsonFunc;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_to_object() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToObjectFunc::new()));
        let q = "SELECT TO_OBJECT('{\"a\": 1, \"b\": 2}') as obj;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------------+",
                "| obj              |",
                "+------------------+",
                "| {\"a\": 1, \"b\": 2} |",
                "+------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_null() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToObjectFunc::new()));
        let q = "SELECT TO_OBJECT(null) as obj;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            ["+-----+", "| obj |", "+-----+", "|     |", "+-----+",],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_to_object_fail() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToObjectFunc::new()));
        let q = "SELECT TO_OBJECT('23') as obj;";
        assert!(ctx.sql(q).await?.collect().await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_object_in_object() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToObjectFunc::new()));
        let q = "SELECT TO_OBJECT('{\"a\": 1, \"b\": {\"a\": 25}}') as obj;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------------------+",
                "| obj                      |",
                "+--------------------------+",
                "| {\"a\": 1, \"b\": {\"a\": 25}} |",
                "+--------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_parse_json() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToObjectFunc::new()));
        ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new(false)));
        let q = "SELECT TO_OBJECT(PARSE_JSON('{\"a\": 1, \"b\": {\"a\": 25}}')) as obj;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------------+",
                "| obj                  |",
                "+----------------------+",
                "| {\"a\":1,\"b\":{\"a\":25}} |",
                "+----------------------+",
            ],
            &result
        );

        Ok(())
    }
}
