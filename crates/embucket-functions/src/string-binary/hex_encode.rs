use crate::string_binary::errors::{
    InvalidArgumentCountSnafu, NonValidASCIIStringSnafu, UnsupportedDataTypeSnafu,
};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringBuilder};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::cast::{
    as_binary_array, as_int64_array, as_large_binary_array, as_large_string_array, as_string_array,
    as_string_view_array,
};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

/// `HEX_ENCODE` function implementation
///
/// Encodes the input using hexadecimal (also 'hex' or 'base16') encoding.
/// The result is comprised of 16 different symbols: The numbers '0' to '9'
/// as well as the letters 'A' to 'F' (or 'a' to 'f', see below).
///
/// Syntax: `HEX_ENCODE(<input> [, <case>])`
///
/// Arguments:
/// - `<input>`: A binary or string expression to be encoded.
/// - `<case>`: This optional boolean argument controls the case of the letters
///   ('A', 'B', 'C', 'D', 'E' and 'F') used in the encoding. The default value
///   is 1 and indicates that uppercase letters are used. The value 0 indicates
///   that lowercase letters are used. All other values are illegal and result in an error.
///
/// Example: `HEX_ENCODE('SNOW')` returns `'534E4F57'`
/// Example: `HEX_ENCODE('SNOW', 0)` returns `'534e4f57'`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HexEncodeFunc {
    signature: Signature,
}

impl Default for HexEncodeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl HexEncodeFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HexEncodeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "hex_encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() || args.len() > 2 {
            return InvalidArgumentCountSnafu {
                function_name: "hex_encode".to_string(),
                expected: "1-2".to_string(),
                actual: args.len(),
            }
            .fail()?;
        }

        // Get the input expression
        let input = args[0].clone();

        // Convert input to array
        let input_array = match input {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => Arc::new(v.to_array()?),
        };

        // Get the case parameter if provided, default to 1 (uppercase)
        let case_array = if args.len() > 1 {
            match args[1].clone() {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(v) => Arc::new(v.to_array()?),
            }
        } else {
            ScalarValue::Int64(Some(1)).to_array_of_size(input_array.len())?
        };

        let case_array = compute::cast(&case_array, &DataType::Int64)?;
        let case_array = as_int64_array(&case_array)?;

        // Process based on input type
        let result = match input_array.data_type() {
            DataType::Utf8 => {
                let string_array = as_string_array(&input_array)?;
                encode_strings_to_hex(string_array.iter(), case_array)?
            }
            DataType::LargeUtf8 => {
                let string_array = as_large_string_array(&input_array)?;
                encode_strings_to_hex(string_array.iter(), case_array)?
            }
            DataType::Utf8View => {
                let string_array = as_string_view_array(&input_array)?;
                encode_strings_to_hex(string_array.iter(), case_array)?
            }
            DataType::Binary => {
                let binary_array = as_binary_array(&input_array)?;
                encode_binary_to_hex(binary_array.iter(), case_array)?
            }
            DataType::LargeBinary => {
                let binary_array = as_large_binary_array(&input_array)?;
                encode_binary_to_hex(binary_array.iter(), case_array)?
            }
            DataType::BinaryView => {
                let binary_view = input_array.as_binary_view();
                let iter = (0..binary_view.len()).map(|i| {
                    if binary_view.is_null(i) {
                        None
                    } else {
                        Some(binary_view.value(i))
                    }
                });
                encode_binary_to_hex(iter, case_array)?
            }
            DataType::Null => ScalarValue::Utf8(None).to_array_of_size(input_array.len())?,
            other => {
                return UnsupportedDataTypeSnafu {
                    function_name: "hex_encode".to_string(),
                    data_type: format!("{other:?}"),
                    expected_types: "string or binary types".to_string(),
                }
                .fail()?;
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Encode string values to hexadecimal
fn encode_strings_to_hex<'a, I>(strings: I, use_uppercase: &Int64Array) -> DFResult<ArrayRef>
where
    I: Iterator<Item = Option<&'a str>>,
{
    let mut builder = StringBuilder::new();

    for (opt_str, uc) in strings.zip(use_uppercase) {
        if let Some(s) = opt_str {
            let hex_string = if uc.is_some_and(|v| v == 1) {
                hex::encode_upper(s.as_bytes())
            } else {
                hex::encode(s.as_bytes())
            };
            // Ensure the hex string is valid UTF-8 (it should always be)
            if hex_string.is_ascii() {
                builder.append_value(hex_string);
            } else {
                return NonValidASCIIStringSnafu.fail()?;
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Encode binary values to hexadecimal
fn encode_binary_to_hex<'a, I>(binaries: I, use_uppercase: &Int64Array) -> DFResult<ArrayRef>
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    let mut builder = StringBuilder::new();

    for (opt_binary, uc) in binaries.zip(use_uppercase) {
        if let Some(binary) = opt_binary {
            let hex_string = if uc.is_some_and(|v| v == 1) {
                hex::encode_upper(binary)
            } else {
                hex::encode(binary)
            };
            // Ensure the hex string is valid UTF-8 (it should always be)
            if hex_string.is_ascii() {
                builder.append_value(hex_string);
            } else {
                return NonValidASCIIStringSnafu.fail()?;
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

crate::macros::make_udf_function!(HexEncodeFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_hex_encode_string_uppercase() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT HEX_ENCODE('SNOW',1) AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| hex_value |",
                "+-----------+",
                "| 534E4F57  |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_string_lowercase() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT HEX_ENCODE('SNOW', 0) AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| hex_value |",
                "+-----------+",
                "| 534e4f57  |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_string_explicit_uppercase() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT HEX_ENCODE('SNOW', 1) AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| hex_value |",
                "+-----------+",
                "| 534E4F57  |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_empty_string() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT HEX_ENCODE('') AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| hex_value |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_null() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT HEX_ENCODE(NULL) AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| hex_value |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_special_characters() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT HEX_ENCODE('Hello, 世界!') AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        // "Hello, 世界!" in UTF-8 bytes: 48656C6C6F2C20E4B896E7958C21
        assert_batches_eq!(
            &[
                "+------------------------------+",
                "| hex_value                    |",
                "+------------------------------+",
                "| 48656C6C6F2C20E4B896E7958C21 |",
                "+------------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_binary_data() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        // Test with binary literal
        let q = "SELECT HEX_ENCODE(X'DEADBEEF') AS hex_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| hex_value |",
                "+-----------+",
                "| DEADBEEF  |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_table_data() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        // Create a table with test data
        let create = "CREATE TABLE test_data (text_col VARCHAR, case_col INT)";
        ctx.sql(create).await?.collect().await?;

        let insert = "INSERT INTO test_data VALUES
                      ('ABC', 1),
                      ('abc', 0),
                      ('123', 1),
                      ('', 1),
                      (NULL, 0)";
        ctx.sql(insert).await?.collect().await?;

        let q = "SELECT
                   text_col,
                   case_col,
                   HEX_ENCODE(text_col, case_col) AS hex_encoded
                 FROM test_data
                 ORDER BY text_col";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+----------+-------------+",
                "| text_col | case_col | hex_encoded |",
                "+----------+----------+-------------+",
                "|          | 1        |             |",
                "| 123      | 1        | 313233      |",
                "| ABC      | 1        | 414243      |",
                "| abc      | 0        | 616263      |",
                "|          | 0        |             |",
                "+----------+----------+-------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hex_encode_various_strings() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexEncodeFunc::new()));

        let q = "SELECT
                   HEX_ENCODE('A') AS single_char,
                   HEX_ENCODE('0') AS digit,
                   HEX_ENCODE(' ') AS space,
                   HEX_ENCODE('\n') AS newline,
                   HEX_ENCODE('\t') AS tab";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------+-------+-------+---------+-----+",
                "| single_char | digit | space | newline | tab |",
                "+-------------+-------+-------+---------+-----+",
                "| 41          | 30    | 20    | 0A      | 09  |",
                "+-------------+-------+-------+---------+-----+",
            ],
            &result
        );
        Ok(())
    }
}
