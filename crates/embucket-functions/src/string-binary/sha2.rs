use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use std::any::Any;
use std::sync::Arc;

/// `SHA2` SQL function
///
/// Returns a hex-encoded string containing the N-bit SHA-2 message digest, where N is the specified output digest size.
/// `SHA2` and `SHA2_HEX` are synonymous functions.
///
/// Syntax: SHA2(<msg> [, <`digest_size`>])
///
/// Arguments:
/// - msg: A string expression, the message to be hashed
/// - `digest_size`: Size (in bits) of the output (224, 256, 384, 512). Default: 256
///
/// Example: SELECT sha2('Embucket', 224) AS value;
///
/// Returns:
/// - Returns a hex-encoded string (VARCHAR) containing the SHA-2 message digest
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Sha2Func {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for Sha2Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha2Func {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("sha2_hex")],
        }
    }
}

impl ScalarUDFImpl for Sha2Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "sha2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;

        if args.is_empty() || args.len() > 2 {
            return exec_err!(
                "SHA2 function requires 1 or 2 arguments, got {}",
                args.len()
            );
        }

        let message = &args[0];
        let digest_size = args.get(1);

        // Determine which digest algorithm to use based on digest size
        let digest_bits = if let Some(size_arg) = digest_size {
            match size_arg {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(size))) => *size,
                ColumnarValue::Array(arr) => {
                    let int_array = as_int64_array(arr)?;
                    if int_array.len() != 1 {
                        return exec_err!("Digest size array must have exactly one element");
                    }
                    int_array.value(0)
                }
                ColumnarValue::Scalar(_) => return exec_err!("Digest size must be an integer"),
            }
        } else {
            256 // Default to SHA-256
        };

        if !matches!(digest_bits, 224 | 256 | 384 | 512) {
            return exec_err!(
                "Invalid digest size: {}. Must be 224, 256, 384, or 512",
                digest_bits
            );
        }

        match message {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) => {
                let hash_result = compute_sha2_hash(text.as_bytes(), digest_bits)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash_result))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let mut builder = StringBuilder::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let text = string_array.value(i);
                        let hash_result = compute_sha2_hash(text.as_bytes(), digest_bits)?;
                        builder.append_value(hash_result);
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(_) => exec_err!("SHA2 function only supports string inputs"),
        }
    }
}

fn compute_sha2_hash(data: &[u8], digest_bits: i64) -> DFResult<String> {
    let hash_bytes = match digest_bits {
        224 => {
            let mut hasher = Sha224::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        256 => {
            let mut hasher = Sha256::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        384 => {
            let mut hasher = Sha384::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        512 => {
            let mut hasher = Sha512::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        _ => return exec_err!("Invalid digest size: {}", digest_bits),
    };

    Ok(hex::encode(hash_bytes))
}

#[must_use]
pub fn get_udf() -> Arc<datafusion_expr::ScalarUDF> {
    Arc::new(datafusion_expr::ScalarUDF::from(Sha2Func::new()))
}
