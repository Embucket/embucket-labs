use super::errors as regexp_errors;
use crate::utils::{pattern_to_regex, regexp, to_string_array};
use datafusion::arrow::array::{BooleanBuilder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::Array;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use snafu::ResultExt;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// `REGEXP_LIKE` function implementation
///
/// Performs a comparison to determine whether a string matches a specified pattern. Both inputs must be text expressions.
/// `REGEXP_LIKE` is similar to the [ NOT ] LIKE function, but with POSIX extended regular expressions instead of SQL LIKE pattern syntax.
/// It supports more complex matching conditions than LIKE.
/// Returns a BOOLEAN or NULL. The value is TRUE if there is a match. Otherwise, returns FALSE.
///
/// Syntax: `REGEXP_LIKE( <subject> , <pattern> [ , <parameters> ] )`
///
/// Arguments:
///
/// `Required`:
/// - `<subject>` the string to search for matches.
/// - `<pattern>` pattern to match.
///
/// `Optional`:
/// - `<parameters>` String of one or more characters that specifies the parameters used for searching for matches.
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
///
/// Example: `REGEXP_LIKE('nevermore1, nevermore2, nevermore3.', 'nevermore')`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpLikeFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for RegexpLikeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpLikeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
            aliases: vec!["rlike".to_string()],
        }
    }
}

impl ScalarUDFImpl for RegexpLikeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "regexp_like"
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
            n if 4 > n && 1 < n => Ok(DataType::Boolean),
            n => regexp_errors::TooManyArgumentsSnafu {
                got: n,
                at_maximum: 3usize,
            }
            .fail()?,
        }
    }

    #[allow(clippy::unwrap_used)]
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

        let parameters = &args.args.get(2).map_or_else(
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

        let mut result_array = BooleanBuilder::with_capacity(array.len());

        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let string_array: &StringArray = &to_string_array(&array)?;
                let regex = pattern_to_regex(&format!("^{pattern}$"), parameters)
                    .context(regexp_errors::UnsupportedRegexSnafu)?;
                regexp(string_array, &regex, 0).for_each(|opt_iter| {
                    result_array.append_option(
                        opt_iter.map(|mut cap_iter| cap_iter.any(|cap| cap.get(0).is_some())),
                    );
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
