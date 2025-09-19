use datafusion::arrow::array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::cast::as_generic_string_array;

use crate::semi_structured::errors;
use crate::string_binary::logical_str;
use datafusion::logical_expr::TypeSignature;
use datafusion_expr::binary::string_coercion;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// `REPLACE` SQL function
///
/// Removes all occurrences of a specified substring, and optionally replaces them with another substring.
///
/// Syntax: `REPLACE( <subject> , <pattern> [ , <replacement> ] )`
///
/// Arguments:
/// - `subject`: The subject is the string in which to do the replacements.
///   Typically, this is a column, but it can be a literal.
/// - `pattern`: This is the substring that you want to replace.
///   Typically, this is a literal, but it can be a column or expression.
/// - `replacement`: This is the value used as a replacement for the pattern.
///   If this is omitted, or is an empty string, then the REPLACE function simply deletes all occurrences of the pattern.
///
/// Example: `SELECT REPLACE('down', 'down', 'up');`
///
/// Returns:
/// - The returned value is the string after all replacements have been done.
#[derive(Debug)]
pub struct ReplaceFunc {
    signature: Signature,
}

impl Default for ReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaceFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![logical_str(), logical_str()]),
                    TypeSignature::Coercible(vec![logical_str(), logical_str(), logical_str()]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReplaceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1]) {
            Ok(coercion_data_type)
        } else {
            errors::ExpectedUtf8StringSnafu.fail()?
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            number_rows,
            return_type,
        } = args;
        let arrays: Vec<_> = args
            .into_iter()
            .map(|arg| arg.into_array(number_rows))
            .collect::<Result<_, _>>()?;

        match return_type {
            DataType::Utf8 | DataType::Utf8View => replace::<i32>(&arrays),
            DataType::LargeUtf8 => replace::<i64>(&arrays),
            _ => errors::ExpectedUtf8StringSnafu.fail()?,
        }
    }
}

fn replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> DFResult<ColumnarValue> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = if args.len() > 2 {
        as_generic_string_array::<T>(&args[2])?.clone()
    } else {
        GenericStringArray::<T>::from(vec![""; string_array.len()])
    };

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => Some(string.replace(from, to)),
            (Some(string), Some(from), None) => Some(string.replace(from, "")),
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

crate::macros::make_udf_function!(ReplaceFunc);
