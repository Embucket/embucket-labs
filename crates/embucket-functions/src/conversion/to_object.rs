use crate::conversion_errors::InvalidTypeSnafu;
use crate::errors::FailedToDeserializeJsonSnafu;
use crate::macros::make_udf_function;
use arrow_schema::DataType;
use datafusion::arrow::array::{Array, StringArray, StringBuilder};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_generic_string_array;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::ResultExt;
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
#[derive(Debug)]
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
        let arr = args[0].clone().into_array(number_rows)?;

        let arr = match arr.data_type() {
            DataType::Null => ScalarValue::Utf8(None).to_array_of_size(arr.len())?,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let arr: &StringArray = as_generic_string_array(&arr)?;
                let mut b = StringBuilder::with_capacity(arr.len(), 1024);

                for v in arr {
                    if let Some(v) = v {
                        let v: Value =
                            serde_json::from_str(v).context(FailedToDeserializeJsonSnafu)?;
                        if let Value::Object(_) = v {
                            b.append_value(v.to_string());
                        } else {
                            return InvalidTypeSnafu.fail()?;
                        }
                    } else {
                        b.append_null();
                    }
                }

                Arc::new(b.finish())
            }
            _ => return InvalidTypeSnafu.fail()?,
        };

        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

make_udf_function!(ToObjectFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversion::to_object::ToObjectFunc;
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
                "+---------------+",
                "| obj           |",
                "+---------------+",
                "| {\"a\":1,\"b\":2} |",
                "+---------------+",
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
}
