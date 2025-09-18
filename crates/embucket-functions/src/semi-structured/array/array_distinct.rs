use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::types::{logical_binary, logical_string};
use datafusion_common::{Result as DFResult, ScalarValue, types::NativeType};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use serde_json::{Value, from_slice, to_string};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayDistinctUDF {
    signature: Signature,
}

impl ArrayDistinctUDF {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Coercible(vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_string()),
                    vec![TypeSignatureClass::Native(logical_binary())],
                    NativeType::String,
                )]),
                volatility: Volatility::Immutable,
            },
        }
    }

    fn distinct_array(string: impl AsRef<str>) -> DFResult<Option<String>> {
        let string = string.as_ref();
        let array_value: Value =
            from_slice(string.as_bytes()).context(errors::FailedToDeserializeJsonSnafu)?;

        if let Value::Array(array) = array_value {
            if array.is_empty() {
                return Ok(Some("[]".to_string()));
            }

            let mut seen = std::collections::HashSet::new();
            let mut distinct_values = Vec::new();

            for value in array {
                if seen.insert(value.clone()) {
                    distinct_values.push(value);
                }
            }

            Ok(Some(
                to_string(&distinct_values).context(errors::SerdeJsonMessageSnafu)?,
            ))
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayDistinctUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayDistinctUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_distinct"
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
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "a variant" }.build())?;

        match array_str {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let str_value = string_array.value(i);
                        results.push(Self::distinct_array(str_value)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringForArraySnafu.fail()?;
                };

                let result = Self::distinct_array(array_str)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }
}

make_udf_function!(ArrayDistinctUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_distinct() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayDistinctUDF::new()));

        // Test with duplicates
        let sql =
            "SELECT array_distinct(array_construct('A', 'A', 'B', NULL, NULL)) as distinct_array";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------------+",
                "| distinct_array |",
                "+----------------+",
                "| [\"A\",\"B\",null] |",
                "+----------------+",
            ],
            &result
        );

        // Test with numbers
        let sql = "SELECT array_distinct(array_construct(1, 2, 1, 3, 2)) as distinct_nums";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| distinct_nums |",
                "+---------------+",
                "| [1,2,3]       |",
                "+---------------+",
            ],
            &result
        );

        // Test empty array
        let sql = "SELECT array_distinct(array_construct()) as empty_array";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| empty_array |",
                "+-------------+",
                "| []          |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
