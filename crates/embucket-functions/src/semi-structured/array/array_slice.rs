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
pub struct ArraySliceUDF {
    signature: Signature,
}

#[allow(
    clippy::as_conversions,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
impl ArraySliceUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(3), // array, from, to
                volatility: Volatility::Immutable,
            },
        }
    }

    fn slice_array(array_value: Value, from: i64, to: i64) -> DFResult<Option<String>> {
        // Ensure the first argument is an array
        if let Value::Array(array) = array_value {
            let array_len = array.len() as i64;

            // Convert negative indices to positive (e.g., -1 means last element)
            let actual_from = if from < 0 { from + array_len } else { from };

            let actual_to = if to < 0 { to + array_len } else { to };

            // Check if indices are valid
            if actual_from < 0
                || actual_from >= array_len
                || actual_to < actual_from
                || actual_to > array_len
            {
                return Ok(None);
            }

            // Extract slice
            let slice = array[actual_from as usize..actual_to as usize].to_vec();

            // Convert back to JSON string
            Ok(Some(
                to_string(&slice).context(errors::FailedToSerializeResultSnafu)?,
            ))
        } else {
            errors::InputMustBeJsonArraySnafu.fail()?
        }
    }
}

impl Default for ArraySliceUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraySliceUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_slice"
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
        let from = args
            .get(1)
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "from" }.build())?;
        let to = args
            .get(2)
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "to" }.build())?;

        match (array_str, from, to) {
            (
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(from_value),
                ColumnarValue::Scalar(to_value),
            ) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                // Get from and to values
                let from = match from_value {
                    ScalarValue::Int64(Some(pos)) => *pos,
                    ScalarValue::Int64(None) | ScalarValue::Null => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => return errors::NamedIndexMustBeAnIntegerSnafu { name: "From" }.fail()?,
                };

                let to = match to_value {
                    ScalarValue::Int64(Some(pos)) => *pos,
                    ScalarValue::Int64(None) | ScalarValue::Null => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => return errors::NamedIndexMustBeAnIntegerSnafu { name: "To" }.fail()?,
                };

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let array_str = string_array.value(i);
                        let array_json: Value =
                            from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;
                        results.push(Self::slice_array(array_json, from, to)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            (
                ColumnarValue::Scalar(array_value),
                ColumnarValue::Scalar(from_value),
                ColumnarValue::Scalar(to_value),
            ) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                };

                // If any argument is NULL, return NULL
                if array_value.is_null() || from_value.is_null() || to_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let from = match from_value {
                    ScalarValue::Int64(Some(pos)) => *pos,
                    _ => return errors::NamedIndexMustBeAnIntegerSnafu { name: "From" }.fail()?,
                };

                let to = match to_value {
                    ScalarValue::Int64(Some(pos)) => *pos,
                    _ => return errors::NamedIndexMustBeAnIntegerSnafu { name: "To" }.fail()?,
                };

                // Parse array string to JSON Value
                let array_json: Value =
                    from_str(array_str).context(errors::FailedToDeserializeJsonSnafu)?;

                let result = Self::slice_array(array_json, from, to)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => errors::FirstArgumentMustBeJsonArrayStringSecondAndThirdIntegersSnafu.fail()?,
        }
    }
}

make_udf_function!(ArraySliceUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_slice() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArraySliceUDF::new()));

        // Test basic slice
        let sql = "SELECT array_slice(array_construct(0, 1, 2, 3, 4, 5, 6), 0, 2) as slice";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------+",
                "| slice |",
                "+-------+",
                "| [0,1] |",
                "+-------+",
            ],
            &result
        );

        // Test slice with negative indices
        let sql = "SELECT array_slice(array_construct('a', 'b', 'c', 'd'), -2, -1) as neg_slice";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| neg_slice |",
                "+-----------+",
                "| [\"c\"]     |",
                "+-----------+",
            ],
            &result
        );

        // Test slice with out of bounds indices
        let sql = "SELECT array_slice(array_construct(1, 2, 3), 5, 7) as invalid_slice";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| invalid_slice |",
                "+---------------+",
                "|               |",
                "+---------------+",
            ],
            &result
        );

        // Test slice with NULL indices
        let sql = "SELECT array_slice(array_construct(1, 2, 3), NULL, 2) as null_slice";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| null_slice |",
                "+------------+",
                "|            |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
