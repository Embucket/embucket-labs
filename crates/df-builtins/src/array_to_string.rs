use datafusion::arrow::array::as_string_array;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::array::StringBuilder;
use datafusion_common::{DataFusionError, ScalarValue, exec_err, internal_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

// array_to_string SQL function
// Converts the input array to a string by first casting each element to a string,
// then concatenating them into a single string with the elements separated by_
//
// Syntax: ARRAY_TO_STRING( <array> , <separator_string> )
//
// Arguments:
// - <array>
// The array of elements to convert to a string.
// - <separator_string>
// The string to use as a separator between elements in the resulting string.
//
// Returns:
// This function returns a result of type STRING.
//
// Usage notes:
// - If any argument is NULL, the function returns NULL.
// - NULL elements within the array are converted to empty strings in the result.
// - To include a space between values, make sure to include the space in the separator itself
//   (e.g., ', '). See the examples below.
#[derive(Debug)]
pub struct ArrayToStringFunc {
    signature: Signature,
}

impl Default for ArrayToStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayToStringFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayToStringFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        match (&args[0], &args[1]) {
            (ColumnarValue::Array(arr), ColumnarValue::Scalar(ScalarValue::Utf8(Some(sep)))) => {
                let mut res = StringBuilder::with_capacity(arr.len(), 1024);
                let arr = as_string_array(arr);
                for v in arr {
                    if let Some(v) = v {
                        let json: Value = serde_json::from_str(v).map_err(|err| {
                            DataFusionError::Execution(format!(
                                "failed to deserialize JSON: {err:?}"
                            ))
                        })?;
                        res.append_value(to_string(&json, sep)?);
                    } else {
                        res.append_null();
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(res.finish())))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(v)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(sep))),
            ) => {
                if let Some(v) = v {
                    let json: Value = serde_json::from_str(v).map_err(|err| {
                        DataFusionError::Execution(format!("failed to deserialize JSON: {err:?}"))
                    })?;
                    let res = to_string(&json, sep)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(res))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
            }
            _ => internal_err!("wrong arguments"),
        }
    }
}

fn to_string(v: &Value, sep: &str) -> DFResult<String> {
    let mut res = vec![];
    match v {
        Value::Array(arr) => {
            for v in arr {
                match v {
                    Value::Bool(v) => res.push(v.to_string()),
                    Value::Number(v) => res.push(v.to_string()),
                    Value::String(v) => res.push(v.to_owned()),
                    Value::Array(v) => {
                        let r = serde_json::to_string(&v).map_err(|err| {
                            DataFusionError::Execution(format!("failed to serialize JSON: {err:?}"))
                        })?;
                        res.push(r);
                    }
                    Value::Object(v) => {
                        let r = serde_json::to_string(&v).map_err(|err| {
                            DataFusionError::Execution(format!("failed to serialize JSON: {err:?}"))
                        })?;
                        res.push(r);
                    }
                    Value::Null => res.push(String::new()),
                }
            }
        }
        _ => return exec_err!("array_to_string expected a array"),
    }

    Ok(res.join(sep))
}

super::macros::make_udf_function!(ArrayToStringFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ArrayToStringFunc::new()));
        let q = r#"SELECT column1,
       ARRAY_TO_STRING(column1, '') AS no_separation,
       ARRAY_TO_STRING(column1, ', ') AS comma_separated
  FROM VALUES
    (NULL),
    ('[]'),
    ('[1]'),
    ('[1, 2]'),
    ('[true, 1, -1.2e-3, "Abc", ["x","y"], { "a":1 }]'),
    ('[true, 1, -1.2e-3, "Abc", ["x","y"], {"a":{"b":"c"},"c":1,"d":[1,2,"3"]}]')"#;
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------------------------------------------------+-------------------------------------------------------------+-----------------------------------------------------------------------+",
                "| column1                                                                   | no_separation                                               | comma_separated                                                       |",
                "+---------------------------------------------------------------------------+-------------------------------------------------------------+-----------------------------------------------------------------------+",
                "|                                                                           |                                                             |                                                                       |",
                "| []                                                                        |                                                             |                                                                       |",
                "| [1]                                                                       | 1                                                           | 1                                                                     |",
                "| [1, 2]                                                                    | 12                                                          | 1, 2                                                                  |",
                r#"| [true, 1, -1.2e-3, "Abc", ["x","y"], { "a":1 }]                           | true1-0.0012Abc["x","y"]{"a":1}                             | true, 1, -0.0012, Abc, ["x","y"], {"a":1}                             |"#,
                r#"| [true, 1, -1.2e-3, "Abc", ["x","y"], {"a":{"b":"c"},"c":1,"d":[1,2,"3"]}] | true1-0.0012Abc["x","y"]{"a":{"b":"c"},"c":1,"d":[1,2,"3"]} | true, 1, -0.0012, Abc, ["x","y"], {"a":{"b":"c"},"c":1,"d":[1,2,"3"]} |"#,
                "+---------------------------------------------------------------------------+-------------------------------------------------------------+-----------------------------------------------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scalar() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ArrayToStringFunc::new()));
        let q = r#"SELECT ARRAY_TO_STRING('[true, 1, -1.2e-3, "Abc", ["x","y"], { "a":1 }]', '') AS no_separation"#;
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------+",
                "| no_separation                   |",
                "+---------------------------------+",
                r#"| true1-0.0012Abc["x","y"]{"a":1} |"#,
                "+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
