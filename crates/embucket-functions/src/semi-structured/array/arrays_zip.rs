use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::Value;
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraysZipUDF {
    signature: Signature,
}

impl ArraysZipUDF {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::VariadicAny,
                volatility: Volatility::Immutable,
            },
        }
    }

    fn zip_arrays(arrays: Vec<Value>) -> DFResult<Option<String>> {
        // If any array is null, return null
        if arrays.iter().any(Value::is_null) {
            return Ok(None);
        }

        // Ensure all inputs are arrays
        let arrays: Vec<Vec<Value>> = arrays
            .into_iter()
            .map(|val| match val {
                Value::Array(arr) => Ok(arr),
                _ => errors::AllArgumentsMustBeArraysSnafu.fail()?,
            })
            .collect::<DFResult<_>>()?;

        // Find the maximum length among all arrays
        let max_len = arrays.iter().map(Vec::len).max().unwrap_or(0);

        // Create the zipped array
        let mut result = Vec::with_capacity(max_len);
        for i in 0..max_len {
            let mut obj = serde_json::Map::new();
            for (array_idx, array) in arrays.iter().enumerate() {
                let key = format!("${}", array_idx + 1);
                let value = array.get(i).cloned().unwrap_or(Value::Null);
                obj.insert(key, value);
            }
            result.push(Value::Object(obj));
        }

        Ok(Some(
            serde_json::to_string(&result).context(errors::FailedToSerializeValueSnafu)?,
        ))
    }
}

impl Default for ArraysZipUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArraysZipUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        match args.first() {
            Some(ColumnarValue::Array(first_array)) => {
                let string_array = first_array.as_string::<i32>();
                let mut results = Vec::new();

                for row in 0..string_array.len() {
                    let mut row_arrays = Vec::new();
                    let mut has_null = false;

                    // Collect all array values for this row
                    for arg in &args {
                        match arg {
                            ColumnarValue::Array(arr) => {
                                let arr = arr.as_string::<i32>();
                                if arr.is_null(row) {
                                    has_null = true;
                                    break;
                                }
                                let array_json: Value = serde_json::from_str(arr.value(row))
                                    .context(errors::FailedToDeserializeJsonSnafu)?;
                                row_arrays.push(array_json);
                            }
                            ColumnarValue::Scalar(_) => {
                                return errors::AllArgumentsMustBeArraysSnafu.fail()?;
                            }
                        }
                    }

                    if has_null {
                        results.push(None);
                    } else {
                        results.push(Self::zip_arrays(row_arrays)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            Some(ColumnarValue::Scalar(_first_value)) => {
                let mut scalar_arrays = Vec::new();

                // If any scalar is NULL, return NULL
                for arg in &args {
                    match arg {
                        ColumnarValue::Scalar(scalar) => {
                            if scalar.is_null() {
                                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                            }
                            if let ScalarValue::Utf8(Some(s)) = scalar {
                                let array_json: Value = serde_json::from_str(s).context(
                                    errors::FailedToDeserializeJsonEntitySnafu { entity: "array" },
                                )?;
                                scalar_arrays.push(array_json);
                            } else {
                                return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                            }
                        }
                        ColumnarValue::Array(_) => {
                            return errors::MixedScalarAndArrayArgumentsNotSupportedSnafu.fail()?;
                        }
                    }
                }

                let result = Self::zip_arrays(scalar_arrays)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            None => errors::ArraysZipRequiresAtLeastOneArrayArgumentSnafu.fail()?,
        }
    }
}

make_udf_function!(ArraysZipUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_arrays_zip() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArraysZipUDF::new()));

        // Test basic zipping of three arrays
        let sql = "SELECT arrays_zip(
            array_construct(1, 2, 3),
            array_construct('first', 'second', 'third'),
            array_construct('i', 'ii', 'iii')
        ) as zipped_arrays";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------------------------------------------------------------------------------------------+",
                "| zipped_arrays                                                                                      |",
                "+----------------------------------------------------------------------------------------------------+",
                "| [{\"$1\":1,\"$2\":\"first\",\"$3\":\"i\"},{\"$1\":2,\"$2\":\"second\",\"$3\":\"ii\"},{\"$1\":3,\"$2\":\"third\",\"$3\":\"iii\"}] |",
                "+----------------------------------------------------------------------------------------------------+",
            ],
            &result
        );

        // Test arrays of different lengths
        let sql = "SELECT arrays_zip(
            array_construct(1, 2, 3),
            array_construct('a', 'b')
        ) as diff_lengths";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------------------------------------------------+",
                "| diff_lengths                                             |",
                "+----------------------------------------------------------+",
                "| [{\"$1\":1,\"$2\":\"a\"},{\"$1\":2,\"$2\":\"b\"},{\"$1\":3,\"$2\":null}] |",
                "+----------------------------------------------------------+",
            ],
            &result
        );

        // Test with NULL array
        let sql = "SELECT arrays_zip(NULL, array_construct(1, 2, 3)) as null_array";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| null_array |",
                "+------------+",
                "|            |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
