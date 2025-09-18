use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_slice, to_string};
use snafu::ResultExt;
use std::sync::Arc;

use crate::json::encode_scalar;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPrependUDF {
    signature: Signature,
}

impl ArrayPrependUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn prepend_element(array_value: &Value, element_value: &Value) -> DFResult<String> {
        // Ensure the first argument is an array
        if let Value::Array(array) = array_value {
            // Create new array with element value prepended
            let mut new_array = Vec::with_capacity(array.len() + 1);
            new_array.push(element_value.clone());
            new_array.extend(array.iter().cloned());

            // Convert back to JSON string
            Ok(to_string(&Value::Array(new_array)).context(errors::FailedToSerializeValueSnafu)?)
        } else {
            errors::ArgumentMustBeJsonArraySnafu { argument: "first" }.fail()?
        }
    }
}

impl Default for ArrayPrependUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayPrependUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_prepend"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array_str = args
            .first()
            .ok_or_else(|| errors::ArrayArgumentExpectedSnafu.build())?;

        let element = args
            .get(1)
            .ok_or_else(|| errors::ExpectedElementArgumentSnafu.build())?;

        match (array_str, element) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(element_value)) => {
                let string_array = array.as_string::<i32>();
                let element_value = encode_scalar(element_value)?;
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_value = from_slice(string_array.value(i).as_bytes())
                            .context(errors::FailedToDeserializeJsonSnafu)?;
                        results.push(Some(Self::prepend_element(&array_value, &element_value)?));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(array_value), ColumnarValue::Scalar(element_value)) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                };
                let array_value = from_slice(array_str.as_bytes())
                    .context(errors::FailedToDeserializeJsonSnafu)?;
                let element_value = encode_scalar(element_value)?;

                let result = Self::prepend_element(&array_value, &element_value)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            _ => errors::FirstArgumentMustBeJsonArrayStringSecondScalarSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayPrependUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_prepend() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayPrependUDF::new()));

        // Test prepending string to numeric array
        let sql = "SELECT array_prepend(array_construct(0,1,2,3), 'hello') as prepended";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------------+",
                "| prepended         |",
                "+-------------------+",
                "| [\"hello\",0,1,2,3] |",
                "+-------------------+",
            ],
            &result
        );

        // Test prepending number
        let sql = "SELECT array_prepend(array_construct(1,2,3), 42) as num_prepend";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| num_prepend |",
                "+-------------+",
                "| [42,1,2,3]  |",
                "+-------------+",
            ],
            &result
        );

        // Test prepending boolean
        let sql = "SELECT array_prepend(array_construct(1,2,3), true) as bool_prepend";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| bool_prepend |",
                "+--------------+",
                "| [true,1,2,3] |",
                "+--------------+",
            ],
            &result
        );

        // Test prepending null
        let sql = "SELECT array_prepend(array_construct(1,2,3), NULL) as null_prepend";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| null_prepend |",
                "+--------------+",
                "| [null,1,2,3] |",
                "+--------------+",
            ],
            &result
        );

        Ok(())
    }
}
