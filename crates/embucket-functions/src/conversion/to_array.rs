use super::errors as conv_errors;
use crate::macros::make_udf_function;
use arrow_schema::Field;
use datafusion::arrow::array::{Array, ListArray, StringArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::ResultExt;
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
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let arr = args[0].clone().into_array(number_rows)?;

        let mut offsets = Vec::with_capacity(arr.len() + 1);
        offsets.push(0);

        let mut flat_values = Vec::new();

        for i in 0..arr.len() {
            let value = ScalarValue::try_from_array(&arr, i)?;
            if value.is_null() {
                offsets.push(
                    i32::try_from(flat_values.len())
                        .context(conv_errors::InvalidIntegerConversionSnafu)?,
                );
                continue;
            }
            let s = value.to_string();
            match serde_json::from_str::<Value>(&s) {
                Ok(v) if v.is_array() => {
                    if let Some(json_arr) = v.as_array() {
                        for item in json_arr {
                            flat_values.push(item.to_string());
                        }
                    }
                }
                _ => flat_values.push(s),
            }
            offsets.push(
                i32::try_from(flat_values.len())
                    .context(conv_errors::InvalidIntegerConversionSnafu)?,
            );
        }
        let values_array = Arc::new(StringArray::from(flat_values));
        let offset_buf = OffsetBuffer::new(offsets.into());

        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            offset_buf,
            values_array,
            None,
        );
        Ok(ColumnarValue::Array(Arc::new(list_array)))
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
                "+----+--------+--------+-----------------------+-----------+-----------+",
                "| a1 | a2     | a3     | a4                    | a5        | a6        |",
                "+----+--------+--------+-----------------------+-----------+-----------+",
                "| [] | [test] | [true] | [1712278923000000000] | [1, 2, 3] | [1, 2, 3] |",
                "+----+--------+--------+-----------------------+-----------+-----------+",
            ],
            &result
        );

        Ok(())
    }
}
