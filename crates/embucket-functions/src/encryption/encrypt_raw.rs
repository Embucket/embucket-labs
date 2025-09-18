use aes::Aes192;
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, KeyInit, Nonce};
use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common;
use datafusion_common::cast::{as_binary_array, as_string_array};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use rand::RngCore;
use serde_json::json;

use std::any::Any;
use std::sync::Arc;

use crate::encryption::errors::{
    ArrayLengthMismatchSnafu, CipherCreationSnafu, EncryptionFailedSnafu,
    InvalidArgumentTypesSnafu, InvalidIvSizeSnafu, InvalidKeySizeSnafu,
    MalformedEncryptionMethodSnafu, UnsupportedEncryptionAlgorithmSnafu,
    UnsupportedEncryptionModeSnafu,
};

type Aes192Gcm = AesGcm<Aes192, aes_gcm::aead::consts::U12>;

/// `ENCRYPT_RAW` SQL function
///
/// Encrypts binary data using AES-GCM algorithm.
///
/// Syntax: `ENCRYPT_RAW(<binary>, <key>, <iv> [, <aad>, <method>])`
///
/// Arguments:
/// - `binary`: binary data to encrypt.
/// - `key`: binary key (16, 24 or 32 bytes).
/// - `iv`: initialization vector (if NULL, generates random IV).
/// - `aad`: optional additional authenticated data.
/// - `method`: optional encryption method, currently only `'AES-GCM'` is supported.
///
/// Example: `SELECT ENCRYPT_RAW(TO_BINARY('abcd','HEX'), key, iv, aad, 'AES-GCM')`;
///
/// Returns:
/// - JSON string containing `ciphertext`, `iv` and `tag` fields encoded in HEX.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EncryptRawFunc {
    signature: Signature,
}

impl Default for EncryptRawFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptRawFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EncryptRawFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "encrypt_raw"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        // Handle NULL values by coercing them to the expected types
        let coerced_types: Vec<DataType> = arg_types
            .iter()
            .enumerate()
            .map(|(i, arg_type)| {
                if arg_type.is_null() {
                    match i {
                        0..=3 => DataType::Binary, // data, key, iv, aad
                        4 => DataType::Utf8,       // method
                        _ => arg_type.clone(),
                    }
                } else {
                    arg_type.clone()
                }
            })
            .collect();

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
        let val_arr = match &args[0] {
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
        let len = val_arr.len();
        if key_arr.len() != len
            || iv_arr.len() != len
            || aad_arr.as_ref().is_some_and(|a| a.len() != len)
            || method_arr.as_ref().is_some_and(|a| a.len() != len)
        {
            ArrayLengthMismatchSnafu.fail()?;
        }
        let vals = as_binary_array(&val_arr)?;
        let keys = as_binary_array(&key_arr)?;
        let ivs = as_binary_array(&iv_arr)?;
        let aads = aad_arr.map(|a| as_binary_array(&a).cloned()).transpose()?;
        let methods = method_arr
            .map(|a| as_string_array(&a).cloned())
            .transpose()?;
        let mut builder = StringBuilder::new();
        for i in 0..len {
            if vals.is_null(i)
                || keys.is_null(i)
                || aads.as_ref().is_some_and(|a| a.is_null(i))
                || methods.as_ref().is_some_and(|m| m.is_null(i))
            {
                builder.append_null();
                continue;
            }
            let value = vals.value(i);
            let key = keys.value(i);
            let aad = aads.as_ref().map_or([].as_slice(), |a| a.value(i));
            let method = methods.as_ref().map_or("AES-GCM", |m| m.value(i));

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

            // Handle NULL IV by generating random IV
            let iv = if ivs.is_null(i) || ivs.value(i).is_empty() {
                generate_random_iv()
            } else {
                ivs.value(i).to_vec()
            };

            // Validate IV size for GCM mode (must be 12 bytes / 96 bits)
            if iv.len() != 12 {
                InvalidIvSizeSnafu {
                    bits: iv.len() * 8,
                    expected_bits: 96_usize,
                    mode: "GCM".to_string(),
                }
                .fail()?;
            }

            let (ciphertext, tag) = match key.len() {
                16 => encrypt::<Aes128Gcm>(value, key, &iv, aad)?,
                24 => encrypt::<Aes192Gcm>(value, key, &iv, aad)?,
                32 => encrypt::<Aes256Gcm>(value, key, &iv, aad)?,
                _ => {
                    return InvalidKeySizeSnafu {
                        bits: key.len() * 8,
                        algorithm: "AES".to_string(),
                    }
                    .fail()?;
                }
            };
            let json = json!({
                "ciphertext": hex::encode_upper(ciphertext),
                "iv": hex::encode_upper(iv),
                "tag": hex::encode_upper(tag),
            });
            builder.append_value(json.to_string());
        }
        let res = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(res))
    }
}

fn generate_random_iv() -> Vec<u8> {
    let mut iv = vec![0u8; 12]; // 96-bit IV for AES-GCM
    rand::rng().fill_bytes(&mut iv);
    iv
}

fn encrypt<C: Aead + KeyInit>(
    value: &[u8],
    key: &[u8],
    iv: &[u8],
    aad: &[u8],
) -> DFResult<(Vec<u8>, Vec<u8>)> {
    // Using map_err instead of .context() to avoid exposing crypto library implementation details
    // and to maintain a clean abstraction layer over the underlying AES-GCM operations
    let cipher = C::new_from_slice(key).map_err(|_| CipherCreationSnafu.build())?;
    let nonce = Nonce::from_slice(iv);
    let payload = Payload { msg: value, aad };
    let mut ct = cipher
        .encrypt(nonce, payload)
        .map_err(|_| EncryptionFailedSnafu.build())?;
    let tag = ct.split_off(ct.len() - 16);
    Ok((ct, tag))
}

crate::macros::make_udf_function!(EncryptRawFunc);
