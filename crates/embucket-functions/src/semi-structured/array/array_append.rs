use crate::json;
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, to_string};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAppendUDF {
    signature: Signature,
}

impl ArrayAppendUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn append_element(array_str: impl AsRef<str>, element: &ScalarValue) -> DFResult<String> {
        let array_str = array_str.as_ref();

        // Parse the input array
        let mut array_value: Value =
            serde_json::from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

        let scalar_value = json::encode_array(element.to_array_of_size(1)?)?;

        let scalar_value = if let Value::Array(array) = scalar_value {
            match array.first() {
                Some(value) => value.clone(),
                None => {
                    return errors::ExpectedArrayForScalarValueSnafu.fail()?;
                }
            }
        } else {
            return errors::ExpectedArrayForScalarValueSnafu.fail()?;
        };
        // Ensure the first argument is an array
        if let Value::Array(ref mut array) = array_value {
            array.push(scalar_value);

            // Convert back to JSON string
            Ok(to_string(&array_value).context(errors::FailedToSerializeValueSnafu)?)
        } else {
            errors::ArgumentMustBeJsonArraySnafu { argument: "First" }.fail()?
        }
    }
}

impl Default for ArrayAppendUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayAppendUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_append"
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
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_value = string_array.value(i);
                        results.push(Some(Self::append_element(array_value, element_value)?));
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

                let result = Self::append_element(array_str, element_value)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            _ => errors::FirstArgumentMustBeJsonArrayStringSecondScalarSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayAppendUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_append() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayAppendUDF::new()));

        // Test appending to numeric array
        let sql = "SELECT array_append(array_construct(1, 2, 3), 4) as appended";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| appended  |",
                "+-----------+",
                "| [1,2,3,4] |",
                "+-----------+",
            ],
            &result
        );

        // Test appending to empty array
        let sql = "SELECT array_append(array_construct(), 1) as empty_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| empty_append |",
                "+--------------+",
                "| [1]          |",
                "+--------------+",
            ],
            &result
        );

        // Test appending string to numeric array
        let sql = "SELECT array_append(array_construct(1, 2), 'hello') as mixed_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| mixed_append  |",
                "+---------------+",
                "| [1,2,\"hello\"] |",
                "+---------------+",
            ],
            &result
        );

        // Test appending boolean
        let sql = "SELECT array_append(array_construct(1, 2), true) as bool_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| bool_append |",
                "+-------------+",
                "| [1,2,true]  |",
                "+-------------+",
            ],
            &result
        );

        // Test appending float
        let sql = "SELECT array_append(array_construct(1, 2), 3.14) as float_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| float_append |",
                "+--------------+",
                "| [1,2,3.14]   |",
                "+--------------+",
            ],
            &result
        );

        // Test appending null
        let sql = "SELECT array_append(array_construct(1, 2), NULL) as null_append";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| null_append |",
                "+-------------+",
                "| [1,2,null]  |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
