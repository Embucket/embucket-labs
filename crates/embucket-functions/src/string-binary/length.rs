use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::{array::UInt64Array, datatypes::DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_common::cast::{
    as_binary_array, as_large_binary_array, as_large_string_array, as_string_array,
    as_string_view_array,
};
use datafusion_common::exec_err;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

/// `LENGTH` SQL function
///
/// Returns the length of a string or binary value.
///
/// Syntax: `LENGTH( <string_expr> )`
///
/// Arguments:
/// - `string_expr`: A string or binary value.
///
/// Example: `SELECT LENGTH('hello') AS value;`
///
/// Returns:
/// - Returns the length of the string or binary value.
#[derive(Debug)]
pub struct LengthFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for LengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LengthFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable,
            },
            aliases: vec![String::from("len")],
        }
    }
}

impl ScalarUDFImpl for LengthFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    #[allow(clippy::as_conversions)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let result = match arr.data_type() {
            DataType::Utf8 => {
                let strs = as_string_array(&arr)?;
                let new_array = strs
                    .iter()
                    .map(|array_elem| array_elem.map(|value| value.chars().count() as u64))
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            DataType::LargeUtf8 => {
                let strs = as_large_string_array(&arr)?;
                let new_array = strs
                    .iter()
                    .map(|array_elem| array_elem.map(|value| value.chars().count() as u64))
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            DataType::Utf8View => {
                let strs = as_string_view_array(&arr)?;
                let new_array = strs
                    .iter()
                    .map(|array_elem| array_elem.map(|value| value.chars().count() as u64))
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            DataType::Binary => {
                let binary = as_binary_array(&arr)?;
                let new_array = binary
                    .iter()
                    .map(|array_elem| array_elem.map(|value| value.len() as u64))
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            DataType::LargeBinary => {
                let binary = as_large_binary_array(&arr)?;
                let new_array = binary
                    .iter()
                    .map(|array_elem| array_elem.map(|value| value.len() as u64))
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            DataType::BinaryView => {
                // Handle BinaryView similar to how it's handled in other functions
                let binary = arr.as_binary_view();
                let new_array = (0..binary.len())
                    .map(|i| {
                        if binary.is_null(i) {
                            None
                        } else {
                            Some(binary.value(i).len() as u64)
                        }
                    })
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            v if v.is_numeric() => {
                let casted = cast(&arr, &DataType::Utf8)?;
                let strs = as_string_array(&casted)?;
                let new_array = strs
                    .iter()
                    .map(|array_elem| array_elem.map(|value| value.chars().count() as u64))
                    .collect::<UInt64Array>();
                Arc::new(new_array)
            }
            DataType::Null => Arc::new((0..arr.len()).map(|_| None).collect::<UInt64Array>()),
            _ => {
                return exec_err!("Invalid datatype");
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

crate::macros::make_udf_function!(LengthFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_string_length() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(LengthFunc::new()));

        let create = "CREATE OR REPLACE TABLE test_strings (s STRING);";
        ctx.sql(create).await?.collect().await?;

        let insert = "
          INSERT INTO test_strings VALUES
              ('hello'),
              ('Joyeux Noël'),
              ('圣诞节快乐'),
              (''),
              (NULL);
          ";
        ctx.sql(insert).await?.collect().await?;

        let q = "SELECT s, LENGTH(s) FROM test_strings;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------+------------------------+",
                "| s           | length(test_strings.s) |",
                "+-------------+------------------------+",
                "| hello       | 5                      |",
                "| Joyeux Noël | 11                     |",
                "| 圣诞节快乐  | 5                      |",
                "|             | 0                      |",
                "|             |                        |",
                "+-------------+------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_utf8_view_length() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(LengthFunc::new()));

        // Create a table with a string column and cast it to Utf8View
        let create =
            "CREATE OR REPLACE TABLE test_utf8_view AS SELECT CAST('hello' AS STRING) AS s;";
        ctx.sql(create).await?.collect().await?;

        // Query using LENGTH on the string column
        let q = "SELECT s, LENGTH(s) FROM test_utf8_view;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+--------------------------+",
                "| s     | length(test_utf8_view.s) |",
                "+-------+--------------------------+",
                "| hello | 5                        |",
                "+-------+--------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_numeric_length() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(LengthFunc::new()));
        let q = "SELECT LENGTH(1) as int_v, LENGTH(1.222) as float_v";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+---------+",
                "| int_v | float_v |",
                "+-------+---------+",
                "| 1     | 5       |",
                "+-------+---------+",
            ],
            &result
        );
        Ok(())
    }
}
