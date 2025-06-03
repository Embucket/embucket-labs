use crate::macros::make_udf_function;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, from_str, to_string};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrayCatUDF {
    signature: Signature,
}

impl ArrayCatUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(2),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn concatenate_arrays(arrays: &[&str]) -> DFResult<String> {
        let mut result_array = Vec::new();

        for array_str in arrays {
            // Parse each input array
            let array_value: Value = from_str(array_str).map_err(|e| {
                datafusion_common::error::DataFusionError::Internal(format!(
                    "Failed to parse array JSON: {e}",
                ))
            })?;

            // Ensure each argument is an array
            if let Value::Array(array) = array_value {
                result_array.extend(array);
            } else {
                return Err(datafusion_common::error::DataFusionError::Internal(
                    "All arguments must be JSON arrays".to_string(),
                ));
            }
        }

        // Convert back to JSON string
        to_string(&Value::Array(result_array)).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to serialize result: {e}",
            ))
        })
    }
}

impl Default for ArrayCatUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayCatUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_cat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Check for exactly two arguments
        if args.len() != 2 {
            return Err(datafusion_common::error::DataFusionError::Internal(
                "array_cat expects exactly two arguments".to_string(),
            ));
        }

        match (&args[0], &args[1]) {
            // Both scalar case
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s1))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2))),
            ) => {
                let result = Self::concatenate_arrays(&[s1.as_str(), s2.as_str()])?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }

            // Scalar + Array case
            (ColumnarValue::Scalar(ScalarValue::Utf8(Some(s1))), ColumnarValue::Array(array2)) => {
                let string_array2 = array2.as_string::<i32>();
                let len = string_array2.len();

                let mut results = Vec::with_capacity(len);
                for i in 0..len {
                    if string_array2.is_null(i) {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Cannot concatenate arrays with null values".to_string(),
                        ));
                    }
                    let result = Self::concatenate_arrays(&[
                        s1.as_str(),
                        string_array2.value(i).to_string().as_str(),
                    ])?;
                    results.push(Some(result));
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }

            // Array + Scalar case
            (ColumnarValue::Array(array1), ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2)))) => {
                let string_array1 = array1.as_string::<i32>();
                let len = string_array1.len();

                let mut results = Vec::with_capacity(len);
                for i in 0..len {
                    if string_array1.is_null(i) {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Cannot concatenate arrays with null values".to_string(),
                        ));
                    }
                    let result = Self::concatenate_arrays(&[
                        string_array1.value(i).to_string().as_str(),
                        s2.as_str(),
                    ])?;
                    results.push(Some(result));
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }

            // Both array case
            (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                let string_array1 = array1.as_string::<i32>();
                let string_array2 = array2.as_string::<i32>();
                let len = string_array1.len();

                let mut results = Vec::with_capacity(len);
                for i in 0..len {
                    if string_array1.is_null(i) || string_array2.is_null(i) {
                        return Err(datafusion_common::error::DataFusionError::Internal(
                            "Cannot concatenate arrays with null values".to_string(),
                        ));
                    }
                    let result = Self::concatenate_arrays(&[
                        string_array1.value(i).to_string().as_str(),
                        string_array2.value(i).to_string().as_str(),
                    ])?;
                    results.push(Some(result));
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }

            _ => Err(datafusion_common::error::DataFusionError::Internal(
                "Arguments must both be either scalar UTF8 strings or arrays".to_string(),
            )),
        }
    }
}

make_udf_function!(ArrayCatUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_cat() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayCatUDF::new()));

        // Test concatenating two arrays
        let sql = "SELECT array_cat(array_construct(1, 2), array_construct(3, 4)) as concatenated";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------------+",
                "| concatenated |",
                "+--------------+",
                "| [1,2,3,4]    |",
                "+--------------+",
            ],
            &result
        );

        // Test concatenating empty arrays
        let sql = "SELECT array_cat(array_construct(), array_construct(1, 2)) as empty_cat";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| empty_cat |",
                "+-----------+",
                "| [1,2]     |",
                "+-----------+",
            ],
            &result
        );

        // Test concatenating arrays with different types
        let sql = "SELECT array_cat(array_construct(1, 2), array_construct('a', 'b')) as mixed_cat";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| mixed_cat     |",
                "+---------------+",
                "| [1,2,\"a\",\"b\"] |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }
}
