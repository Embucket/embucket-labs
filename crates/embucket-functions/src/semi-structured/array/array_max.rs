use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::types::{NativeType, logical_binary, logical_string};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use serde_json::{Value, from_slice};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayMaxUDF {
    signature: Signature,
}

impl ArrayMaxUDF {
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

    fn find_max(string: impl AsRef<str>) -> DFResult<Option<String>> {
        let string = string.as_ref();
        let array_value: Value =
            from_slice(string.as_bytes()).context(errors::FailedToDeserializeJsonSnafu)?;

        if let Value::Array(array) = array_value {
            if array.is_empty() {
                return Ok(None);
            }

            // Try to find the maximum value, handling different types
            let mut max_value: Option<String> = None;
            let mut max_type: Option<&str> = None;

            for value in array {
                match value {
                    Value::Number(n) if n.is_i64() => {
                        let num = n
                            .as_i64()
                            .ok_or_else(|| errors::FailedToParseNumberSnafu.build())?;
                        let should_update = match max_value.as_ref() {
                            None => true,
                            Some(current) => {
                                if let Ok(current_num) = current.parse::<i64>() {
                                    max_type == Some("i64") && num > current_num
                                } else {
                                    false
                                }
                            }
                        };
                        if should_update {
                            max_value = Some(num.to_string());
                            max_type = Some("i64");
                        }
                    }
                    Value::Number(n) if n.is_f64() => {
                        let num = n
                            .as_f64()
                            .ok_or_else(|| errors::FailedToParseNumberSnafu.build())?;
                        let should_update = match max_value.as_ref() {
                            None => true,
                            Some(current) => {
                                if let Ok(current_num) = current.parse::<f64>() {
                                    max_type == Some("f64") && num > current_num
                                } else {
                                    false
                                }
                            }
                        };
                        if should_update {
                            max_value = Some(num.to_string());
                            max_type = Some("f64");
                        }
                    }
                    _ => {}
                }
            }

            Ok(max_value)
        } else {
            Ok(None)
        }
    }
}

impl Default for ArrayMaxUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayMaxUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_max"
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
            .ok_or_else(|| errors::ExpectedNamedArgumentSnafu { name: "variant" }.build())?;
        match array_str {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut results = Vec::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        results.push(None);
                    } else {
                        let str_value = string_array.value(i);
                        results.push(Self::find_max(str_value)?);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
            ColumnarValue::Scalar(array_value) => {
                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedUtf8StringSnafu.fail()?;
                };

                let result = Self::find_max(array_str)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }
}

make_udf_function!(ArrayMaxUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_max() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayMaxUDF::new()));

        // Test numeric array
        let sql = "SELECT array_max(array_construct(1, 5, 3, 9, 2)) as max_num";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| max_num |",
                "+---------+",
                "| 9       |",
                "+---------+",
            ],
            &result
        );

        // Test mixed types
        let sql = "SELECT array_max(array_construct(1, 'hello', 2.5, 10)) as max_mixed";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| max_mixed |",
                "+-----------+",
                "| 10        |",
                "+-----------+",
            ],
            &result
        );

        // Test array of nulls
        let sql = "SELECT array_max(array_construct(NULL, NULL, NULL)) as null_max";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| null_max |",
                "+----------+",
                "|          |",
                "+----------+"
            ],
            &result
        );

        // Test empty array
        let sql = "SELECT array_max(array_construct()) as empty_max";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------+",
                "| empty_max |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
