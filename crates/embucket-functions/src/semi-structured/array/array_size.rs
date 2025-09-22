use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_str};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraySizeUDF {
    signature: Signature,
}

#[allow(clippy::as_conversions, clippy::cast_possible_wrap)]
impl ArraySizeUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn get_array_size(value: Value) -> Option<i64> {
        match value {
            Value::Array(array) => Some(array.len() as i64),
            _ => None, // Return NULL for non-array values
        }
    }
}

impl Default for ArraySizeUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraySizeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_arg = args
            .first()
            .ok_or_else(|| errors::ArrayArgumentExpectedSnafu.build())?;

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

                        results.push(Self::get_array_size(array_json));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::Int64Array::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => match array_value {
                ScalarValue::Utf8(Some(s)) => {
                    let array_json: Value =
                        from_str(s).context(errors::FailedToDeserializeJsonSnafu)?;

                    let size = Self::get_array_size(array_json);
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(size)))
                }
                ScalarValue::Utf8(None) | ScalarValue::Null => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)))
                }
                _ => errors::ExpectedUtf8StringSnafu.fail()?,
            },
        }
    }
}

make_udf_function!(ArraySizeUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_size() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArraySizeUDF::new()));

        // Test empty array
        let sql = "SELECT array_size(array_construct()) as empty_size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| empty_size |",
                "+------------+",
                "| 0          |",
                "+------------+",
            ],
            &result
        );

        // Test array with elements
        let sql = "SELECT array_size(array_construct(1, 2, 3, 4)) as size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            ["+------+", "| size |", "+------+", "| 4    |", "+------+",],
            &result
        );

        // Test with non-array input
        let sql = "SELECT array_size('\"not an array\"') as invalid_size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| invalid_size |",
                "+--------------+",
                "|              |",
                "+--------------+",
            ],
            &result
        );

        // Test with NULL input
        let sql = "SELECT array_size(NULL) as null_size";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| null_size |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
