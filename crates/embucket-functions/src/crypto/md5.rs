use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::functions::crypto::basic::{DigestAlgorithm, digest_process};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::cast::as_binary_array;
use datafusion_common::utils::take_function_args;
use datafusion_expr::ScalarFunctionArgs;
use std::any::Any;
use std::fmt::Write;
use std::sync::Arc;

/// `MD5` SQL function
///
/// Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest.
///
/// Syntax: `MD5`(<msg>), `MD5_HEX`(<msg>)
///
/// Arguments
/// - `msg`: A string expression, the message to be hashed.
///
/// Returns a 32-character hex-encoded string.
#[derive(Debug)]
pub struct Md5Func {
    signature: Signature,
    aliases: Vec<String>,
}
impl Default for Md5Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5Func {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
            aliases: vec!["md5_hex".to_string()],
        }
    }
}
impl ScalarUDFImpl for Md5Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "md5"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { number_rows, .. } = args;
        let [data] = take_function_args("md5", args.args)?;
        let mut arr = data.into_array(number_rows)?;

        if !matches!(
            arr.data_type(),
            DataType::Utf8View
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        ) {
            arr = cast(&arr, &DataType::Utf8)?;
        }
        let data = ColumnarValue::Array(Arc::new(arr));
        let value = digest_process(&data, DigestAlgorithm::Md5)?.into_array(number_rows)?;

        let binary_array = as_binary_array(&value)?;
        let string_array: StringArray = binary_array
            .iter()
            .map(|opt| opt.map(hex_encode::<_>))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(string_array)))
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    for b in data.as_ref() {
        // Writing to a string never errors, so we can unwrap here.
        write!(&mut s, "{b:02x}").unwrap_or_default();
    }
    s
}

crate::macros::make_udf_function!(Md5Func);
