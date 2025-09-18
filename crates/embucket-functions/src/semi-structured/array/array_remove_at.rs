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
pub struct ArrayRemoveAtUDF {
    signature: Signature,
}

#[allow(
    clippy::cast_possible_wrap,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
impl ArrayRemoveAtUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn remove_at_position(array_value: Value, position: i64) -> DFResult<Option<String>> {
        // Ensure the first argument is an array
        if let Value::Array(mut array) = array_value {
            let array_len = array.len() as i64;

            // Convert negative index to positive (e.g., -1 means last element)
            let actual_pos = if position < 0 {
                position + array_len
            } else {
                position
            };

            // Check if position is valid
            if actual_pos < 0 || actual_pos >= array_len {
                return Ok(None);
            }

            // Remove element at position
            array.remove(actual_pos as usize);

            // Convert back to JSON string
            Ok(Some(
                to_string(&array).context(errors::FailedToSerializeValueSnafu)?,
            ))
        } else {
            errors::ArgumentMustBeJsonArraySnafu { argument: "first" }.fail()?
        }
    }
}

impl Default for ArrayRemoveAtUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayRemoveAtUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_remove_at"
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

        let position = args
            .get(1)
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "position" }.build())?;

        match (array_str, position) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(position_value)) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                // Get position value
                let position = match position_value {
                    ScalarValue::Int64(Some(pos)) => *pos,
                    ScalarValue::Int64(None) | ScalarValue::Null => {
                        // If position is NULL, return NULL
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return errors::PositionMustBeAnIntegerSnafu.fail()?;
                    }
                };

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_str = string_array.value(i);
                        let array_json: Value =
                            from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                        results.push(Self::remove_at_position(array_json, position)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(array_value), ColumnarValue::Scalar(position_value)) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                };

                // If either array or position is NULL, return NULL
                if array_value.is_null() || position_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let position = match position_value {
                    ScalarValue::Int64(Some(pos)) => *pos,
                    _ => return errors::PositionMustBeAnIntegerSnafu.fail()?,
                };

                // Parse array string to JSON Value
                let array_json: Value =
                    from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                let result = Self::remove_at_position(array_json, position)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => errors::FirstArgumentMustBeJsonArrayStringSecondIntegerSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayRemoveAtUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_remove_at() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayRemoveAtUDF::new()));

        // Test removing at position 0
        let sql = "SELECT array_remove_at(array_construct(2, 5, 7), 0) as removed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| removed |",
                "+---------+",
                "| [5,7]   |",
                "+---------+",
            ],
            &result
        );

        // Test removing at last position
        let sql = "SELECT array_remove_at(array_construct('a', 'b', 'c'), 2) as last_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| last_remove |",
                "+-------------+",
                "| [\"a\",\"b\"]   |",
                "+-------------+",
            ],
            &result
        );

        // Test removing at middle position
        let sql = "SELECT array_remove_at(array_construct(true, false, true), 1) as middle_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| middle_remove |",
                "+---------------+",
                "| [true,true]   |",
                "+---------------+",
            ],
            &result
        );

        // Test removing with negative index
        let sql = "SELECT array_remove_at(array_construct(1, 2, 3), -1) as neg_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| neg_remove |",
                "+------------+",
                "| [1,2]      |",
                "+------------+",
            ],
            &result
        );

        // Test removing with out of bounds index
        let sql = "SELECT array_remove_at(array_construct(1, 2, 3), 5) as invalid_remove";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------+",
                "| invalid_remove |",
                "+----------------+",
                "|                |",
                "+----------------+",
            ],
            &result
        );

        // Test removing with NULL position
        let sql = "SELECT array_remove_at(array_construct(1, 2, 3), NULL) as null_pos";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| null_pos |",
                "+----------+",
                "|          |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }
}
