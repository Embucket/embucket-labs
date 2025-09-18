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
pub struct ArrayInsertUDF {
    signature: Signature,
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::as_conversions,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
impl ArrayInsertUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(3),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn insert_element(
        array_str: impl AsRef<str>,
        pos: i64,
        element: &ScalarValue,
    ) -> DFResult<String> {
        let array_str = array_str.as_ref();

        // Parse the input array
        let mut array_value: Value =
            serde_json::from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

        let scalar_value = json::encode_scalar(element)?;

        // Ensure the first argument is an array
        if let Value::Array(ref mut array) = array_value {
            // Convert position to usize, handling negative indices
            let pos = if pos < 0 {
                (array.len() as i64 + pos).max(0) as usize
            } else {
                pos as usize
            };

            // Ensure position is within bounds
            if pos > array.len() {
                return errors::PositionOutOfBoundsForArrayLengthSnafu {
                    pos,
                    length: array.len(),
                }
                .fail()?;
            }

            array.insert(pos, scalar_value);

            // Convert back to JSON string
            let res = to_string(&array_value).context(errors::FailedToSerializeValueSnafu)?;
            Ok(res)
        } else {
            errors::ArgumentMustBeJsonArraySnafu { argument: "First" }.fail()?
        }
    }
}

impl Default for ArrayInsertUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayInsertUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_insert"
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
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "array" }.build())?;
        let pos = args
            .get(1)
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "position" }.build())?;
        let element = args
            .get(2)
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "element" }.build())?;

        match (array_str, pos, element) {
            (
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(pos_value),
                ColumnarValue::Scalar(element_value),
            ) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                // Get position as i64
                let ScalarValue::Int64(Some(pos)) = pos_value else {
                    return errors::PositionMustBeAnIntegerSnafu.fail()?;
                };

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_value = string_array.value(i);
                        results.push(Some(Self::insert_element(
                            array_value,
                            *pos,
                            element_value,
                        )?));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            (
                ColumnarValue::Scalar(array_value),
                ColumnarValue::Scalar(pos_value),
                ColumnarValue::Scalar(element_value),
            ) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                };

                let ScalarValue::Int64(Some(pos)) = pos_value else {
                    return errors::PositionMustBeAnIntegerSnafu.fail()?;
                };

                let result = Self::insert_element(array_str, *pos, element_value)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            _ => errors::FirstArgumentMustBeJsonArrayStringSecondIntegerThirdScalarSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayInsertUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_insert() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayInsertUDF::new()));

        // Test inserting into numeric array
        let sql = "SELECT array_insert(array_construct(0,1,2,3), 2, 'hello') as inserted";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------------+",
                "| inserted          |",
                "+-------------------+",
                "| [0,1,\"hello\",2,3] |",
                "+-------------------+",
            ],
            &result
        );

        // Test inserting at the beginning
        let sql = "SELECT array_insert(array_construct(1,2,3), 0, 'start') as start_insert";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------------+",
                "| start_insert    |",
                "+-----------------+",
                "| [\"start\",1,2,3] |",
                "+-----------------+",
            ],
            &result
        );

        // Test inserting at the end
        let sql = "SELECT array_insert(array_construct(1,2,3), 3, 'end') as end_insert";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| end_insert    |",
                "+---------------+",
                "| [1,2,3,\"end\"] |",
                "+---------------+",
            ],
            &result
        );

        // Test inserting number
        let sql = "SELECT array_insert(array_construct(1,2,3), 1, 42) as num_insert";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| num_insert |",
                "+------------+",
                "| [1,42,2,3] |",
                "+------------+",
            ],
            &result
        );

        // Test inserting boolean
        let sql = "SELECT array_insert(array_construct(1,2,3), 1, true) as bool_insert";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| bool_insert  |",
                "+--------------+",
                "| [1,true,2,3] |",
                "+--------------+",
            ],
            &result
        );

        // Test inserting null
        let sql = "SELECT array_insert(array_construct(1,2,3), 1, NULL) as null_insert";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| null_insert  |",
                "+--------------+",
                "| [1,null,2,3] |",
                "+--------------+",
            ],
            &result
        );

        Ok(())
    }
}
