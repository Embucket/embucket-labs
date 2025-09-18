use super::errors as conv_errors;
use base64::Engine;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::cast::{
    as_binary_array, as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::types::logical_string;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

/// `TO_BINARY` function implementation
///
/// Converts the input expression to a binary value.
/// For NULL input, the output is NULL.
///
/// Syntax: `TO_BINARY(<expr> [, <format>])`
///
/// Arguments:
/// - `<expr>`: The expression to convert to binary. If the expression is NULL, it returns NULL.
/// - `<format>`: Optional format specifier. Valid values are:
///   - 'HEX': Interprets the input as a hexadecimal string (default if not specified)
///   - 'BASE64': Interprets the input as a base64-encoded string
///   - 'UTF-8': Interprets the input as a UTF-8 string
///
/// Example: `TO_BINARY('SNOW', 'UTF-8')`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToBinaryFunc {
    signature: Signature,
    try_mode: bool,
}

impl Default for ToBinaryFunc {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ToBinaryFunc {
    #[must_use]
    pub fn new(try_mode: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // TO_BINARY(<string>)
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_string()),
                    )]),
                    // TO_BINARY(<string>, <format>)
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            try_mode,
        }
    }
}

impl ScalarUDFImpl for ToBinaryFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.try_mode {
            "try_to_binary"
        } else {
            "to_binary"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Get the input expression
        let input = args[0].clone();

        // Get the format if provided, default to HEX
        let format = if args.len() > 1 {
            match &args[1] {
                ColumnarValue::Scalar(v) => v.to_string(),
                ColumnarValue::Array(arr) => {
                    if arr.len() == 1 && !arr.is_null(0) {
                        let format_arr = as_string_array(arr)?;
                        format_arr.value(0).to_string()
                    } else {
                        return conv_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
                    }
                }
            }
        } else {
            "HEX".to_string()
        };

        // Convert input to array
        let input_array = match input {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => Arc::new(v.to_array()?),
        };

        // Process based on input type and format
        let result = match input_array.data_type() {
            DataType::Utf8 => {
                let string_array = as_string_array(&input_array)?;
                convert_string_to_binary(string_array.iter(), format, self.try_mode)?
            }
            DataType::LargeUtf8 => {
                let string_array = as_large_string_array(&input_array)?;
                convert_string_to_binary(string_array.iter(), format, self.try_mode)?
            }
            DataType::Utf8View => {
                let string_array = as_string_view_array(&input_array)?;
                convert_string_to_binary(string_array.iter(), format, self.try_mode)?
            }
            DataType::Binary => {
                // If input is already binary, return as is
                input_array
            }
            DataType::LargeBinary => {
                // Convert LargeBinary to Binary
                let binary_array = as_binary_array(&input_array)?;
                let mut builder = BinaryBuilder::new();
                for i in 0..binary_array.len() {
                    if binary_array.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(binary_array.value(i));
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::BinaryView => {
                // Convert BinaryView to Binary
                let binary_view = input_array.as_binary_view();
                let mut builder = BinaryBuilder::new();
                for i in 0..binary_view.len() {
                    if binary_view.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(binary_view.value(i));
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                return conv_errors::UnsupportedInputTypeSnafu {
                    data_type: input_array.data_type().clone(),
                }
                .fail()?;
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Strip surrounding quotes if they are properly matched
fn strip_surrounding_quotes(s: &str) -> &str {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

fn convert_string_to_binary<'a, I>(strings: I, format: String, try_mode: bool) -> DFResult<ArrayRef>
where
    I: Iterator<Item = Option<&'a str>>,
{
    let format_upper = format.to_uppercase();

    let mut builder = BinaryBuilder::new();

    for opt_str in strings {
        if let Some(s) = opt_str {
            match format_upper.to_lowercase().as_str() {
                "hex" => {
                    // Convert hex string to binary
                    // Strip surrounding quotes if present (for JSON compatibility)
                    let s = strip_surrounding_quotes(s);
                    let s = s.replace(' ', ""); // Remove spaces
                    match hex::decode(s) {
                        Ok(bytes) => builder.append_value(&bytes),
                        Err(e) => {
                            if try_mode {
                                builder.append_null();
                            } else {
                                return conv_errors::FailedToDecodeHexStringSnafu {
                                    error: e.to_string(),
                                }
                                .fail()?;
                            }
                        }
                    }
                }
                "base64" => {
                    // Convert base64 string to binary
                    // Strip surrounding quotes if present (for JSON compatibility)
                    let s = strip_surrounding_quotes(s);
                    match base64::engine::general_purpose::STANDARD.decode(s) {
                        Ok(bytes) => builder.append_value(&bytes),
                        Err(e) => {
                            if try_mode {
                                builder.append_null();
                            } else {
                                return conv_errors::FailedToDecodeBase64StringSnafu {
                                    error: e.to_string(),
                                }
                                .fail()?;
                            }
                        }
                    }
                }
                "utf-8" | "utf8" => {
                    // Convert UTF-8 string to binary
                    builder.append_value(s.as_bytes());
                }
                _ => {
                    if try_mode {
                        builder.append_null();
                    } else {
                        return conv_errors::UnsupportedFormatSnafu {
                            format,
                            expected: "HEX, BASE64, and UTF-8".to_string(),
                        }
                        .fail()?;
                    }
                }
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_to_binary_utf8() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(false)));

        let q = "SELECT TO_BINARY('SNOW', 'UTF-8') AS binary_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------+",
                "| binary_value |",
                "+--------------+",
                "| 534e4f57     |",
                "+--------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_to_binary_default() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(false)));

        let q = "SELECT TO_BINARY('534E4F57') AS binary_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------+",
                "| binary_value |",
                "+--------------+",
                "| 534e4f57     |",
                "+--------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_to_binary_hex() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(false)));

        let q = "SELECT TO_BINARY('534E4F57', 'HEX') AS binary_value";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------+",
                "| binary_value |",
                "+--------------+",
                "| 534e4f57     |",
                "+--------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_to_binary_base64() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(false)));

        let q = "SELECT TO_BINARY('U05PVw==', 'BASE64') AS binary_value";
        let result = ctx.sql(q).await?.collect().await?;

        // The result should be the binary representation of the base64 string "U05PVw==" (which is "SNOW")
        assert_batches_eq!(
            &[
                "+--------------+",
                "| binary_value |",
                "+--------------+",
                "| 534e4f57     |",
                "+--------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_to_binary_null() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(false)));

        let q = "SELECT TO_BINARY(NULL, 'UTF-8') AS binary_value";
        let result = ctx.sql(q).await?.collect().await?;

        // The result should be NULL
        assert_batches_eq!(
            &[
                "+--------------+",
                "| binary_value |",
                "+--------------+",
                "|              |",
                "+--------------+"
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_try_to_binary() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(true)));

        // Valid conversion
        let q = "SELECT TRY_TO_BINARY('534E4F57', 'HEX') AS valid";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+----------+",
                "| valid    |",
                "+----------+",
                "| 534e4f57 |",
                "+----------+",
            ],
            &result
        );

        // Invalid conversion - should return NULL instead of error
        let q = "SELECT TRY_TO_BINARY('ZZZZ', 'HEX') AS invalid";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+---------+",
                "| invalid |",
                "+---------+",
                "|         |",
                "+---------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_to_binary_table() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBinaryFunc::new(false)));

        // Create a table with different string types
        let create = "CREATE TABLE test_strings (s VARCHAR, hex VARCHAR, b64 VARCHAR)";
        ctx.sql(create).await?.collect().await?;

        // Insert test data
        let insert = "INSERT INTO test_strings VALUES ('SNOW', '534E4F57', 'U05PVw==')";
        ctx.sql(insert).await?.collect().await?;

        // Test with different formats
        let q = "SELECT 
                  TO_BINARY(s, 'UTF-8') AS utf8_bin,
                  TO_BINARY(hex, 'HEX') AS hex_bin,
                  TO_BINARY(b64, 'BASE64') AS b64_bin
                FROM test_strings";
        let result = ctx.sql(q).await?.collect().await?;

        // All should result in the same binary value
        assert_batches_eq!(
            &[
                "+----------+----------+----------+",
                "| utf8_bin | hex_bin  | b64_bin  |",
                "+----------+----------+----------+",
                "| 534e4f57 | 534e4f57 | 534e4f57 |",
                "+----------+----------+----------+",
            ],
            &result
        );
        Ok(())
    }
}
