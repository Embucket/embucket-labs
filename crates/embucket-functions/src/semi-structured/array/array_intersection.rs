use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_slice};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayIntersectionUDF {
    signature: Signature,
}

impl ArrayIntersectionUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn array_intersection(
        array1_str: Option<&str>,
        array2_str: Option<&str>,
    ) -> DFResult<Option<Value>> {
        if let (Some(arr1), Some(arr2)) = (array1_str, array2_str) {
            // Parse both arrays
            let array1_value: Value =
                from_slice(arr1.as_bytes()).context(errors::FailedToDeserializeJsonSnafu)?;

            let array2_value: Value =
                from_slice(arr2.as_bytes()).context(errors::FailedToDeserializeJsonSnafu)?;

            if let (Value::Array(arr1), Value::Array(arr2)) = (array1_value, array2_value) {
                // Create a new array with elements that exist in both arr1 and arr2
                let result: Vec<Value> = arr2
                    .into_iter()
                    .filter(|item| arr1.contains(item))
                    .collect();

                Ok(Some(Value::Array(result)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayIntersectionUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayIntersectionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_intersection"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array1 = args.first().ok_or_else(|| {
            errors::ExpectedNamedArgumentSnafu {
                name: "first array",
            }
            .build()
        })?;
        let array2 = args.get(1).ok_or_else(|| {
            errors::ExpectedNamedArgumentSnafu {
                name: "second array",
            }
            .build()
        })?;

        match (array1, array2) {
            (ColumnarValue::Array(array1_array), ColumnarValue::Array(array2_array)) => {
                let array1_strings = array1_array.as_string::<i32>();
                let array2_strings = array2_array.as_string::<i32>();
                let mut results = Vec::new();

                for (arr1, arr2) in array1_strings.iter().zip(array2_strings) {
                    let result = Self::array_intersection(arr1, arr2)?;
                    results.push(result.map(|v| serde_json::to_string(&v)).transpose());
                }

                let results: Result<Vec<Option<String>>, serde_json::Error> =
                    results.into_iter().collect();
                let results = results.context(errors::FailedToSerializeResultSnafu)?;

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(array1_scalar), ColumnarValue::Scalar(array2_scalar)) => {
                let array1_str = match array1_scalar {
                    ScalarValue::Utf8(Some(s)) => s,
                    ScalarValue::Null | ScalarValue::Utf8(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return errors::ExpectedUtf8StringForNamedArraySnafu { name: "first" }
                            .fail()?;
                    }
                };

                let array2_str = match array2_scalar {
                    ScalarValue::Utf8(Some(s)) => s,
                    ScalarValue::Null | ScalarValue::Utf8(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return errors::ExpectedUtf8StringForNamedArraySnafu { name: "second" }
                            .fail()?;
                    }
                };

                let result = Self::array_intersection(Some(array1_str), Some(array2_str))?;
                let result = result
                    .map(|v| serde_json::to_string(&v))
                    .transpose()
                    .context(errors::FailedToSerializeResultSnafu)?;

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => errors::MismatchedArgumentTypesSnafu.fail()?,
        }
    }
}

make_udf_function!(ArrayIntersectionUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_intersection() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayIntersectionUDF::new()));

        // Test basic array intersection
        let sql = "SELECT array_intersection(array_construct('A', 'B'), array_construct('B', 'C')) as result1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result1 |",
                "+---------+",
                "| [\"B\"]   |",
                "+---------+",
            ],
            &result
        );

        // Test empty intersection
        let sql = "SELECT array_intersection(array_construct('A', 'B'), array_construct('C', 'D')) as result2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| result2 |",
                "+---------+",
                "| []      |",
                "+---------+",
            ],
            &result
        );

        // Test with null values
        let sql = "SELECT array_intersection(array_construct('A', NULL), array_construct('A', NULL)) as result3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| result3    |",
                "+------------+",
                "| [\"A\",null] |",
                "+------------+",
            ],
            &result
        );

        // Test with duplicate values
        let sql = "SELECT array_intersection(array_construct('A', 'B', 'B', 'B', 'C'), array_construct('B', 'B')) as result4";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| result4   |",
                "+-----------+",
                "| [\"B\",\"B\"] |",
                "+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
