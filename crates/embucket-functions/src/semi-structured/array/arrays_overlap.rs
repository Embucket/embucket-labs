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
pub struct ArraysOverlapUDF {
    signature: Signature,
}

impl ArraysOverlapUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn arrays_have_overlap(array1: Value, array2: Value) -> DFResult<Option<bool>> {
        // Ensure both arguments are arrays
        if let (Value::Array(arr1), Value::Array(arr2)) = (array1, array2) {
            // Convert arrays to HashSet for efficient comparison
            let set1: std::collections::HashSet<String> =
                arr1.iter().map(ToString::to_string).collect();

            // Check if any element from arr2 exists in set1
            for val in arr2 {
                if set1.contains(&val.to_string()) {
                    return Ok(Some(true));
                }
            }

            Ok(Some(false))
        } else {
            errors::BothArgumentsMustBeJsonArraysSnafu.fail()?
        }
    }
}

impl Default for ArraysOverlapUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraysOverlapUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "arrays_overlap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let array1_arg = args.first().ok_or_else(|| {
            errors::ExpectedNamedArgumentSnafu {
                name: "first array",
            }
            .build()
        })?;
        let array2_arg = args.get(1).ok_or_else(|| {
            errors::ExpectedNamedArgumentSnafu {
                name: "second array",
            }
            .build()
        })?;

        match (array1_arg, array2_arg) {
            (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                let string_array1 = array1.as_string::<i32>();
                let string_array2 = array2.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array1.len() {
                    if string_array1.is_null(i) || string_array2.is_null(i) {
                        results.push(None);
                    } else {
                        let array1_str = string_array1.value(i);
                        let array2_str = string_array2.value(i);

                        let array1_json: Value = from_str(array1_str).context(
                            errors::FailedToDeserializeJsonEntitySnafu {
                                entity: "first array",
                            },
                        )?;

                        let array2_json: Value = from_str(array2_str).context(
                            errors::FailedToDeserializeJsonEntitySnafu {
                                entity: "second array",
                            },
                        )?;

                        results.push(Self::arrays_have_overlap(array1_json, array2_json)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::BooleanArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(array1_value), ColumnarValue::Scalar(array2_value)) => {
                // If either array is NULL, return NULL
                if array1_value.is_null() || array2_value.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                let ScalarValue::Utf8(Some(array1_str)) = array1_value else {
                    return errors::ExpectedUtf8StringForNamedArraySnafu {
                        name: "first array",
                    }
                    .fail()?;
                };
                let ScalarValue::Utf8(Some(array2_str)) = array2_value else {
                    return errors::ExpectedUtf8StringForNamedArraySnafu {
                        name: "second array",
                    }
                    .fail()?;
                };

                // Parse array strings to JSON Values
                let array1_json: Value =
                    from_str(array1_str).context(errors::FailedToDeserializeJsonEntitySnafu {
                        entity: "first array",
                    })?;

                let array2_json: Value =
                    from_str(array2_str).context(errors::FailedToDeserializeJsonEntitySnafu {
                        entity: "second array",
                    })?;

                let result = Self::arrays_have_overlap(array1_json, array2_json)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
            _ => errors::BothArgumentsMustBeJsonArraysSnafu.fail()?,
        }
    }
}

make_udf_function!(ArraysOverlapUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_arrays_overlap() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArraysOverlapUDF::new()));

        // Test with string arrays that overlap
        let sql = "SELECT arrays_overlap(array_construct('hello', 'aloha'), array_construct('hello', 'hi', 'hey')) as overlap";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| overlap |",
                "+---------+",
                "| true    |",
                "+---------+",
            ],
            &result
        );

        // Test with string arrays that don't overlap
        let sql = "SELECT arrays_overlap(array_construct('hello', 'aloha'), array_construct('hola', 'bonjour', 'ciao')) as overlap";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| overlap |",
                "+---------+",
                "| false   |",
                "+---------+",
            ],
            &result
        );

        // Test with NULL values
        let sql = "SELECT arrays_overlap(NULL, array_construct(1, 2, 3)) as overlap";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| overlap |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );

        Ok(())
    }
}
