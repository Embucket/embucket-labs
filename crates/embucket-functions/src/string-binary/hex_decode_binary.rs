use crate::string_binary::errors::{IllegalHexValueSnafu, UnsupportedDataTypeSnafu};
use datafusion::arrow::array::{Array, ArrayRef, BinaryBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::str::from_utf8;
use std::sync::Arc;

/// `HEX_DECODE_BINARY` function implementation
///
/// Decodes a hex-encoded string to a binary.
/// The input must be a valid hexadecimal string (containing only 0-9, A-F, a-f)
///
/// Syntax: `HEX_DECODE_BINARY(<hex_string>)`
///
/// Arguments:
/// - `<hex_string>`: A string containing hexadecimal characters to be decoded.
///
/// Returns: Binary
///
/// Example: `HEX_DECODE_BINARY('534E4F57')` returns binary data for 'SNOW'
/// Example: `HEX_DECODE_BINARY('48656C6C6F')` returns binary data for 'Hello'
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HexDecodeBinaryFunc {
    signature: Signature,
    try_mode: bool,
}

impl Default for HexDecodeBinaryFunc {
    fn default() -> Self {
        Self::new(false)
    }
}

impl HexDecodeBinaryFunc {
    #[must_use]
    pub fn new(try_mode: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // HEX_DECODE_BINARY(<string>)
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_string()),
                    )]),
                ],
                Volatility::Immutable,
            ),
            try_mode,
        }
    }
}

impl ScalarUDFImpl for HexDecodeBinaryFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.try_mode {
            "try_hex_decode_binary"
        } else {
            "hex_decode_binary"
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

        // Convert input to array
        let input_array = match input {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => Arc::new(v.to_array()?),
        };

        // Process based on input type
        let result = match input_array.data_type() {
            DataType::Utf8 => {
                let string_array = as_string_array(&input_array)?;
                decode_hex_strings(string_array.iter(), self.try_mode)?
            }
            DataType::LargeUtf8 => {
                let string_array = as_large_string_array(&input_array)?;
                decode_hex_strings(string_array.iter(), self.try_mode)?
            }
            DataType::Utf8View => {
                let string_array = as_string_view_array(&input_array)?;
                decode_hex_strings(string_array.iter(), self.try_mode)?
            }
            DataType::Null => ScalarValue::Utf8(None).to_array_of_size(input_array.len())?,
            other => {
                return UnsupportedDataTypeSnafu {
                    function_name: self.name().to_string(),
                    data_type: format!("{other:?}"),
                    expected_types: "string".to_string(),
                }
                .fail()?;
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Decode hex strings to binary data
fn decode_hex_strings<'a, I>(strings: I, try_mode: bool) -> DFResult<ArrayRef>
where
    I: Iterator<Item = Option<&'a str>>,
{
    let mut builder = BinaryBuilder::new();

    for opt_str in strings {
        if let Some(hex_str) = opt_str {
            if hex_str.is_empty() {
                builder.append_value([]);
                continue;
            }
            // Attempt to decode the hex string
            match hex::decode(hex_str) {
                Ok(binary_data) => match from_utf8(binary_data.as_slice()) {
                    Ok(s) => {
                        builder.append_value(s);
                    }
                    Err(_) => {
                        if try_mode {
                            builder.append_null();
                        } else {
                            return IllegalHexValueSnafu {
                                value: hex_str.to_string(),
                            }
                            .fail()?;
                        }
                    }
                },
                Err(_) => {
                    if try_mode {
                        builder.append_null();
                    } else {
                        return IllegalHexValueSnafu {
                            value: hex_str.to_string(),
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
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexDecodeBinaryFunc::new(false)));

        let q = "SELECT HEX_DECODE_BINARY('534E4F57')";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------------------+",
                "| hex_decode_binary(Utf8(\"534E4F57\")) |",
                "+-------------------------------------+",
                "| 534e4f57                            |",
                "+-------------------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_lowercase() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexDecodeBinaryFunc::new(false)));

        let q = "SELECT HEX_DECODE_BINARY('534e4f57')";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------------------+",
                "| hex_decode_binary(Utf8(\"534e4f57\")) |",
                "+-------------------------------------+",
                "| 534e4f57                            |",
                "+-------------------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_empty() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexDecodeBinaryFunc::new(false)));

        let q = "SELECT HEX_DECODE_BINARY('')";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------------------+",
                "| hex_decode_binary(Utf8(\"\")) |",
                "+-----------------------------+",
                "|                             |",
                "+-----------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_null() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexDecodeBinaryFunc::new(false)));

        let q = "SELECT HEX_DECODE_BINARY(NULL)";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------+",
                "| hex_decode_binary(NULL) |",
                "+-------------------------+",
                "|                         |",
                "+-------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexDecodeBinaryFunc::new(false)));

        let q = "SELECT HEX_DECODE_BINARY('invalid')";
        assert!(ctx.sql(q).await?.collect().await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_try() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(HexDecodeBinaryFunc::new(true)));

        let q = "SELECT TRY_HEX_DECODE_BINARY('invalid')";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------------------------------------+",
                "| try_hex_decode_binary(Utf8(\"invalid\")) |",
                "+----------------------------------------+",
                "|                                        |",
                "+----------------------------------------+",
            ],
            &result
        );
        Ok(())
    }
}
