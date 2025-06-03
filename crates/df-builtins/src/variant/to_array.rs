use crate::macros::make_udf_function;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::StringBuilder;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

/// `TO_ARRAY` function
///
/// Converts the input expression to an ARRAY value.
///
/// Syntax: `TO_ARRAY(<expr>)`
///
/// Arguments:
/// - `<expr>`: The expression to convert to an array. If the expression is NULL, it returns NULL.
///
/// Example: `TO_ARRAY('test')`
///
/// Returns:
/// This function returns either an ARRAY or NULL:
/// - If the input is an ARRAY or a VARIANT holding an ARRAY, it returns the value as-is.
/// - If the input is NULL or a JSON null, the function returns NULL.
/// - For all other input types, the function returns a single-element ARRAY containing the input value.
#[derive(Debug)]
pub struct ToArrayFunc {
    signature: Signature,
}

impl Default for ToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToArrayFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut b = StringBuilder::with_capacity(arr.len(), 1024);
        for i in 0..arr.len() {
            let value = ScalarValue::try_from_array(&arr, i)?;
            if value.is_null() {
                b.append_null();
                continue;
            }
            match value.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    match serde_json::from_str::<Value>(&value.to_string()) {
                        Ok(v) => {
                            if v.is_array() {
                                b.append_value(v.to_string());
                            } else {
                                b.append_value(format!("[\"{value}\"]"));
                            }
                        }
                        Err(_) => b.append_value(format!("[\"{value}\"]")),
                    }
                }
                _ => b.append_value(format!("[{value}]")),
            }
        }

        let res = b.finish();
        Ok(if res.len() == 1 {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?));
        } else {
            ColumnarValue::Array(Arc::new(b.finish()))
        })
    }
}

make_udf_function!(ToArrayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToArrayFunc::new()));
        let q = "SELECT TO_ARRAY(NULL) as a1,\
        TO_ARRAY('test') as a2, \
        TO_ARRAY(true) as a3, \
        TO_ARRAY('2024-04-05 01:02:03'::TIMESTAMP) as a4, \
        TO_ARRAY([1,2,3]) as a5, \
        TO_ARRAY('[1,2,3]') as a6;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----+----------+--------+-----------------------+-------------+---------+",
                "| a1 | a2       | a3     | a4                    | a5          | a6      |",
                "+----+----------+--------+-----------------------+-------------+---------+",
                "|    | [\"test\"] | [true] | [1712278923000000000] | [[1, 2, 3]] | [1,2,3] |",
                "+----+----------+--------+-----------------------+-------------+---------+",
            ],
            &result
        );

        Ok(())
    }
}
