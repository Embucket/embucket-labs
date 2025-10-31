use aes::Aes192;
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, KeyInit, Nonce};
use datafusion::arrow::array::{Array, BinaryBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::cast::{as_binary_array, as_string_array};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};

use crate::encryption::errors::{
    ArrayLengthMismatchSnafu, CiphertextTooShortSnafu, DecryptionFailedSnafu,
    InvalidArgumentTypesSnafu, InvalidIvSizeSnafu, InvalidKeyLengthSnafu,
    MalformedEncryptionMethodSnafu, NullIvForDecryptionSnafu, SliceCreationFailedSnafu,
    UnsupportedEncryptionAlgorithmSnafu, UnsupportedEncryptionModeSnafu,
};
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

type Aes192Gcm = AesGcm<Aes192, aes_gcm::aead::consts::U12>;

/// `DECRYPT_RAW` SQL function
///
/// Decrypts ciphertext produced by `ENCRYPT_RAW` using AES-GCM.
///
/// Syntax: `DECRYPT_RAW(<ciphertext>, <key>, <iv> [, <aad>, <method>, <tag>])`
///
/// Arguments:
/// - `ciphertext`: encrypted bytes.
/// - `key`: encryption key (16, 24 or 32 bytes).
/// - `iv`: initialization vector.
/// - `aad`: optional additional authenticated data.
/// - `method`: encryption method, only `'AES-GCM'` supported.
/// - `tag`: authentication tag.
///
/// Returns: decrypted binary data.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct DecryptRawFunc {
    signature: Signature,
}

impl Default for DecryptRawFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecryptRawFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecryptRawFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "decrypt_raw"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    #[allow(clippy::too_many_lines)]
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        // Handle NULL values by coercing them to the expected types
        let coerced_types: Vec<DataType> = arg_types
            .iter()
            .enumerate()
            .map(|(i, arg_type)| {
                if arg_type.is_null() {
                    match i {
                        0 | 1 | 2 | 3 | 5 => DataType::Binary, // ciphertext, key, iv, aad, tag
                        4 => DataType::Utf8,                   // method
                        _ => arg_type.clone(),
                    }
                } else {
                    arg_type.clone()
                }
            })
            .collect();

        // Validate the coerced signature matches one of our expected signatures
        let format_types = |types: &[DataType]| -> String {
            format!(
                "({})",
                types
                    .iter()
                    .map(|t| format!("{t:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        match coerced_types.len() {
            3 => {
                if matches!(coerced_types[0], DataType::Binary)
                    && matches!(coerced_types[1], DataType::Binary)
                    && matches!(coerced_types[2], DataType::Binary)
                {
                    Ok(coerced_types)
                } else {
                    InvalidArgumentTypesSnafu {
                        function_name: self.name().to_uppercase(),
                        types: format_types(arg_types),
                    }
                    .fail()?
                }
            }
            4 => {
                if matches!(coerced_types[0], DataType::Binary)
                    && matches!(coerced_types[1], DataType::Binary)
                    && matches!(coerced_types[2], DataType::Binary)
                    && matches!(coerced_types[3], DataType::Binary)
                {
                    Ok(coerced_types)
                } else {
                    InvalidArgumentTypesSnafu {
                        function_name: self.name().to_uppercase(),
                        types: format_types(arg_types),
                    }
                    .fail()?
                }
            }
            5 => {
                if matches!(coerced_types[0], DataType::Binary)
                    && matches!(coerced_types[1], DataType::Binary)
                    && matches!(coerced_types[2], DataType::Binary)
                    && matches!(coerced_types[3], DataType::Binary)
                    && matches!(coerced_types[4], DataType::Utf8)
                {
                    Ok(coerced_types)
                } else {
                    InvalidArgumentTypesSnafu {
                        function_name: self.name().to_uppercase(),
                        types: format_types(arg_types),
                    }
                    .fail()?
                }
            }
            6 => {
                if matches!(coerced_types[0], DataType::Binary)
                    && matches!(coerced_types[1], DataType::Binary)
                    && matches!(coerced_types[2], DataType::Binary)
                    && matches!(coerced_types[3], DataType::Binary)
                    && matches!(coerced_types[4], DataType::Utf8)
                    && matches!(coerced_types[5], DataType::Binary)
                {
                    Ok(coerced_types)
                } else {
                    InvalidArgumentTypesSnafu {
                        function_name: self.name().to_uppercase(),
                        types: format_types(arg_types),
                    }
                    .fail()?
                }
            }
            _ => InvalidArgumentTypesSnafu {
                function_name: self.name().to_uppercase(),
                types: format_types(arg_types),
            }
            .fail()?,
        }
    }

    #[allow(clippy::too_many_lines, clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let ct_arr = match &args[0] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let key_arr = match &args[1] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let iv_arr = match &args[2] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let aad_arr = if args.len() > 3 {
            match &args[3] {
                ColumnarValue::Array(a) => Some(a.clone()),
                ColumnarValue::Scalar(v) => Some(v.to_array()?),
            }
        } else {
            None
        };
        let method_arr = if args.len() > 4 {
            match &args[4] {
                ColumnarValue::Array(a) => Some(a.clone()),
                ColumnarValue::Scalar(v) => Some(v.to_array()?),
            }
        } else {
            None
        };
        let tag_arr = if args.len() > 5 {
            match &args[5] {
                ColumnarValue::Array(a) => Some(a.clone()),
                ColumnarValue::Scalar(v) => Some(v.to_array()?),
            }
        } else {
            None
        };
        let len = ct_arr.len();
        if key_arr.len() != len
            || iv_arr.len() != len
            || aad_arr.as_ref().is_some_and(|a| a.len() != len)
            || method_arr.as_ref().is_some_and(|a| a.len() != len)
            || tag_arr.as_ref().is_some_and(|a| a.len() != len)
        {
            ArrayLengthMismatchSnafu.fail()?;
        }
        let cts = as_binary_array(&ct_arr)?;
        let keys = as_binary_array(&key_arr)?;
        let ivs = as_binary_array(&iv_arr)?;
        let aads = aad_arr.map(|a| as_binary_array(&a).cloned()).transpose()?;
        let methods = method_arr
            .map(|a| as_string_array(&a).cloned())
            .transpose()?;
        let tags = tag_arr.map(|a| as_binary_array(&a).cloned()).transpose()?;
        let mut builder = BinaryBuilder::new();
        for i in 0..len {
            if ivs.is_null(i) {
                NullIvForDecryptionSnafu.fail()?;
            }

            if cts.is_null(i) || keys.is_null(i) {
                builder.append_null();
                continue;
            }

            // For optional parameters, allow null but treat as default values
            if aads.as_ref().is_some_and(|a| a.is_null(i))
                || methods.as_ref().is_some_and(|m| m.is_null(i))
                || tags.as_ref().is_some_and(|t| t.is_null(i))
            {
                builder.append_null();
                continue;
            }
            let ct = cts.value(i);
            let key = keys.value(i);
            let iv = ivs.value(i);
            let aad = aads.as_ref().map_or([].as_slice(), |a| a.value(i));
            let method = methods.as_ref().map_or("AES-GCM", |m| m.value(i));
            let tag = tags.as_ref().map_or([].as_slice(), |t| t.value(i));

            // Validate IV size for GCM mode (must be 12 bytes / 96 bits)
            if iv.len() != 12 {
                InvalidIvSizeSnafu {
                    bits: iv.len() * 8,
                    expected_bits: 96_usize,
                    mode: "GCM".to_string(),
                }
                .fail()?;
            }

            let method_upper = method.to_uppercase();
            let parts: Vec<&str> = method_upper.split('-').collect();
            if parts.len() != 2 {
                MalformedEncryptionMethodSnafu {
                    method: method.to_string(),
                }
                .fail()?;
            }

            let algorithm = parts[0];
            let mode = parts[1];

            if algorithm != "AES" {
                UnsupportedEncryptionAlgorithmSnafu {
                    algorithm: algorithm.to_string(),
                }
                .fail()?;
            }

            if mode != "GCM" {
                UnsupportedEncryptionModeSnafu {
                    mode: mode.to_string(),
                }
                .fail()?;
            }

            let (ciphertext_data, tag_data): (&[u8], &[u8]) = if tag.is_empty() {
                if ct.len() < 16 {
                    CiphertextTooShortSnafu.fail()?;
                }
                let (ct_part, tag_part) = ct.split_at(ct.len() - 16);
                (ct_part, tag_part)
            } else {
                (ct, tag)
            };

            // Create combined ciphertext+tag for AES-GCM decryption
            let mut combined_ct = Vec::from(ciphertext_data);
            combined_ct.extend_from_slice(tag_data);

            let pt = match key.len() {
                16 => decrypt::<Aes128Gcm>(&combined_ct, key, iv, aad)?,
                24 => decrypt::<Aes192Gcm>(&combined_ct, key, iv, aad)?,
                32 => decrypt::<Aes256Gcm>(&combined_ct, key, iv, aad)?,
                _ => InvalidKeyLengthSnafu { length: key.len() }.fail()?,
            };
            builder.append_value(pt);
        }
        let arr = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(arr))
    }
}

fn decrypt<C: Aead + KeyInit>(ct: &[u8], key: &[u8], iv: &[u8], aad: &[u8]) -> DFResult<Vec<u8>> {
    // Using map_err instead of .context() to avoid exposing crypto library implementation details
    // and to maintain a clean abstraction layer over the underlying AES-GCM operations
    let cipher = C::new_from_slice(key).map_err(|_| DecryptionFailedSnafu.build())?;
    let nonce = Nonce::<C::NonceSize>::try_from(iv).context(SliceCreationFailedSnafu)?;
    let payload = Payload { msg: ct, aad };
    let pt = cipher
        .decrypt(&nonce, payload)
        .map_err(|_| DecryptionFailedSnafu.build())?;
    Ok(pt)
}

crate::macros::make_udf_function!(DecryptRawFunc);
