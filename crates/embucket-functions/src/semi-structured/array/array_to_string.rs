use crate::json;
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::as_string_array;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::array::StringBuilder;
use datafusion_common::internal_err;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

/// `array_to_string` SQL function
/// Converts the input array to a string by first casting each element to a string,
/// then concatenating them into a single string with the elements separated by_
///
/// Syntax: `ARRAY_TO_STRING`( <`array`> , <`separator_string`> )
///
/// Arguments:
/// - <`array`> The array of elements to convert to a string.
/// - <`separator_string`> The string to use as a separator between elements in the resulting string.
///
/// Returns:
/// This function returns a result of type STRING.
///
/// Usage notes:
/// - If any argument is NULL, the function returns NULL.
/// - NULL elements within the array are converted to empty strings in the result.
/// - To include a space between values, make sure to include the space in the separator itself
///   (e.g., ', '). See the examples below.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayToStringFunc {
    signature: Signature,
}

impl Default for ArrayToStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayToStringFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
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
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        let arrays: Vec<_> = args
            .into_iter()
            .map(|arg| arg.into_array(number_rows))
            .collect::<Result<_, _>>()?;
        let json_array = json::encode_array(arrays[0].clone())?;
        let sep_array = as_string_array(&arrays[1]);

        let mut res = StringBuilder::with_capacity(arrays[0].len(), 1024);
        if let Value::Array(v) = &json_array {
            for (row_index, row) in v.iter().enumerate() {
                let sep = sep_array.value(row_index);
                match &row {
                    Value::Null => res.append_null(),
                    _ => res.append_value(to_string(row, sep)?),
                }
            }
        } else {
            return internal_err!("wrong arguments");
        }

        Ok(ColumnarValue::Array(Arc::new(res.finish())))
    }
}

fn to_string(v: &Value, sep: &str) -> DFResult<String> {
    let mut res = vec![];
    match v {
        Value::Array(arr) => {
            for av in arr {
                match av {
                    Value::Array(_) | Value::Object(_) => {
                        res.push(
                            serde_json::to_string(av)
                                .context(errors::FailedToSerializeValueSnafu)?,
                        );
                    }
                    _ => res.push(to_string(av, sep)?),
                }
            }
        }
        Value::Object(v) => {
            res.push(serde_json::to_string(&v).context(errors::FailedToSerializeValueSnafu)?);
        }
        Value::Null => res.push(String::new()),
        Value::Bool(v) => res.push(v.to_string()),
        Value::Number(v) => res.push(v.to_string()),
        Value::String(v) => {
            if let Ok(json) = serde_json::from_str::<Value>(v) {
                res.push(to_string(&json, sep)?);
            } else {
                res.push(v.to_owned());
            }
        }
    }
    Ok(res.join(sep))
}

make_udf_function!(ArrayToStringFunc);

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
                r#"| [true, 1, -1.2e-3, "Abc", ["x","y"], { "a":1 }]                           | true1-1.2e-3Abc["x","y"]{"a":1}                             | true, 1, -1.2e-3, Abc, ["x","y"], {"a":1}                             |"#,
                r#"| [true, 1, -1.2e-3, "Abc", ["x","y"], {"a":{"b":"c"},"c":1,"d":[1,2,"3"]}] | true1-1.2e-3Abc["x","y"]{"a":{"b":"c"},"c":1,"d":[1,2,"3"]} | true, 1, -1.2e-3, Abc, ["x","y"], {"a":{"b":"c"},"c":1,"d":[1,2,"3"]} |"#,
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
                r#"| true1-1.2e-3Abc["x","y"]{"a":1} |"#,
                "+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
