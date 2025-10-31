use super::errors as regexp_errors;
use crate::utils::{pattern_to_regex, regexp, to_string_array};
use datafusion::arrow::array::{StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::Array;
use datafusion_common::types::logical_string;
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl};
use snafu::ResultExt;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// `REGEXP_SUBSTR_ALL` function implementation
///
/// Returns an ARRAY that contains all substrings that match a regular expression within a string.
/// The function returns a value of type ARRAY. The array contains an element for each matching substring.
/// The function returns an empty array if no match is found.
///
/// Syntax: `REGEXP_SUBSTR_ALL( <subject> , <pattern> [ , <position> [ , <occurrence> [ , <regex_parameters> [ , <group_num> ] ] ] ] )`
///
/// Arguments:
///
/// `Required`:
/// - `<subject>` the string to search for matches.
/// - `<pattern>` pattern to match.
///
/// `Optional`:
/// - `<position>` number of characters from the beginning of the string where the function starts searching for matches.
///   Default: `1` (the search for a match starts at the first character on the left)
/// - `<occurrence>` specifies the first occurrence of the pattern from which to start returning matches.
///   The function skips the first occurrence - 1 matches. For example, if there are 5 matches and you specify 3 for the occurrence argument,
///   the function ignores the first two matches and returns the third, fourth, and fifth matches.
///   Default: `1`
/// - `<regex_parameters>` String of one or more characters that specifies the parameters used for searching for matches.
///   Supported values:
///   ---------------------------------------------------------------------------
///   | Parameter       | Description                               |
///   |-----------------|-------------------------------------------|
///   | c               | Case-sensitive matching                   |
///   | i               | Case-insensitive matching                 |
///   | m               | Multi-line mode                           |
///   | e               | Extract submatches                        |
///   | s               | POSIX wildcard character `.` matches `\n` |
///   ---------------------------------------------------------------------------
///   Default: `c`
/// - `<group_num>` the `group_num` parameter specifies which group to extract.
///   Groups are specified by using parentheses in the regular expression.
///   If a `group_num` is specified, it allows extraction even if the e option was not also specified.
///   The e option is implied.
///
/// Example: `REGEXP_SUBSTR_ALL('nevermore1, nevermore2, nevermore3.', 'nevermore')`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpSubstrAllFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for RegexpSubstrAllFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpSubstrAllFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("regexp_extract_all")],
        }
    }
    #[allow(clippy::too_many_lines, clippy::unwrap_used)]
    fn take_args_values(args: &[ColumnarValue]) -> DFResult<(usize, usize, &str, usize)> {
        let position = args.get(2).map_or_else(
            || Ok(0),
            |value| match value {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if 0 <= *value => {
                    usize::try_from(*value - 1)
                        .context(regexp_errors::InvalidIntegerConversionSnafu)
                }
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if 0 > *value => {
                    regexp_errors::WrongArgValueSnafu {
                        got: value.to_string(),
                        reason: "Position must be positive".to_string(),
                    }
                    .fail()
                }
                other => regexp_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.data_type(),
                    position: 3usize,
                }
                .fail(),
            },
        )?;

        let occurrence = args.get(3).map_or_else(
            || Ok(0),
            |value| match value {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if 0 <= *value => {
                    usize::try_from(*value - 1)
                        .context(regexp_errors::InvalidIntegerConversionSnafu)
                }
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if 0 > *value => {
                    regexp_errors::WrongArgValueSnafu {
                        got: value.to_string(),
                        reason: "Occurrence must be positive".to_string(),
                    }
                    .fail()
                }
                other => regexp_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.data_type(),
                    position: 4usize,
                }
                .fail(),
            },
        )?;

        let regex_parameters = args.get(4).map_or_else(
            || Ok("c"),
            |value| match value {
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::Utf8View(Some(value))
                    | ScalarValue::LargeUtf8(Some(value)),
                ) if value.contains(['c', 'i', 'm', 'e', 's']) => Ok(value),
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::Utf8View(Some(value))
                    | ScalarValue::LargeUtf8(Some(value)),
                ) if value.is_empty() => Ok("c"),
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::Utf8View(Some(value))
                    | ScalarValue::LargeUtf8(Some(value)),
                ) => regexp_errors::WrongArgValueSnafu {
                    got: value.clone(),
                    //We just checked if value is empty, if not - this is valid, since we are getting here the excluded range so just the zeroes character
                    reason: format!("Unknown parameter: '{}'", value.get(0..1).unwrap()),
                }
                .fail(),
                other => regexp_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.data_type(),
                    position: 5usize,
                }
                .fail(),
            },
        )?;

        let group_num = args.get(5).map_or_else(
            || {
                if regex_parameters.contains('e') {
                    Ok(1)
                } else {
                    Ok(0)
                }
            },
            |value| match value {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if 0 <= *value => {
                    usize::try_from(*value).context(regexp_errors::InvalidIntegerConversionSnafu)
                }
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if 0 > *value => {
                    regexp_errors::WrongArgValueSnafu {
                        got: value.to_string(),
                        reason: "Capture group mustbe non-negative".to_string(),
                    }
                    .fail()
                }
                other => regexp_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.data_type(),
                    position: 6usize,
                }
                .fail(),
            },
        )?;

        Ok((position, occurrence, regex_parameters, group_num))
    }
}

impl ScalarUDFImpl for RegexpSubstrAllFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "regexp_substr_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match arg_types.len() {
            0 => regexp_errors::NotEnoughArgumentsSnafu {
                got: 0usize,
                at_least: 2usize,
            }
            .fail()?,
            //Return type specified as Number, probably an `Integer` which is an alias to `Number(38, 0)`,
            // we return `Int64` for better internal DF compatibility
            n if 7 > n && 1 < n => Ok(DataType::Utf8),
            n => regexp_errors::TooManyArgumentsSnafu {
                got: n,
                at_maximum: 6usize,
            }
            .fail()?,
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        //Already checked that it's at least > 1
        let subject = &args.args[0];
        let array = match subject {
            ColumnarValue::Array(array) => array,
            //Can't fail (shouldn't)
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        //Already checked that it's at least > 1
        let pattern = match &args.args[1] {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(pattern))
                | ScalarValue::LargeUtf8(Some(pattern))
                | ScalarValue::Utf8View(Some(pattern)),
            ) => pattern,
            other => {
                return regexp_errors::UnsupportedInputTypeWithPositionSnafu {
                    data_type: other.data_type(),
                    position: 2usize,
                }
                .fail()?;
            }
        };

        let (position, occurrence, regex_parameters, group_num) =
            Self::take_args_values(&args.args)?;

        //TODO: Or data_capacity: 1024
        let mut result_array = StringBuilder::with_capacity(array.len(), array.len() * 10);

        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let string_array: &StringArray = &to_string_array(&array)?;
                let regex = pattern_to_regex(pattern, regex_parameters)
                    .context(regexp_errors::UnsupportedRegexSnafu)?;
                regexp(string_array, &regex, position)
                    .enumerate()
                    .for_each(|(index, opt_iter)| {
                        result_array.append_option(opt_iter.map(|cap_iter| {
                            //Can't panic
                            let mut result =
                                String::with_capacity(string_array.value(index).len()) + "[";
                            cap_iter.skip(occurrence).for_each(|cap| {
                                //group_num == 0, means get the whole match (seems docs in regex are incorrect)
                                if let Some(mat) = cap.get(group_num) {
                                    result += "\"";
                                    result += mat.as_str();
                                    result += "\",";
                                }
                            });
                            //to avoid an `if > 0` in for each match, we remove the last ","
                            if result.len() > 1 {
                                result.pop();
                            }
                            result += "]";
                            //Only reallocate if we are smaller then or equal 50% of the capacity,
                            if result.capacity() / 2 >= result.len() {
                                result.shrink_to_fit();
                            }
                            result
                        }));
                    });
            }
            other => regexp_errors::UnsupportedInputTypeWithPositionSnafu {
                position: 1usize,
                data_type: other.clone(),
            }
            .fail()?,
        }

        Ok(ColumnarValue::Array(Arc::new(result_array.finish())))
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
