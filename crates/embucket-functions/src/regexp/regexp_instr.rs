use super::errors as regexp_errors;
use crate::utils::{pattern_to_regex, regexp};
use datafusion::arrow::array::{Decimal128Builder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::Array;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::types::logical_string;
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl};
use snafu::ResultExt;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

///TODO: Docs
#[derive(Debug)]
pub struct RegexpInstrFunc {
    signature: Signature,
}

impl Default for RegexpInstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpInstrFunc {
    #[must_use]
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
                        Coercion::new_exact(TypeSignatureClass::Integer),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Integer),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpInstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "regexp_instr"
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
            //TODO: or Int8..64? Return type specified as Number, probably Integer as alias to Number(38, 0)
            n if 8 > n && 1 < n => Ok(DataType::Decimal128(38, 0)),
            n => regexp_errors::TooManyArgumentsSnafu {
                got: n,
                at_maximum: 7usize,
            }
            .fail()?,
        }
    }

    //TODO: clippy fix conversions
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::as_conversions
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        //Already checked that it's at least > 1
        let subject = &args.args[0];
        let array = match subject {
            ColumnarValue::Array(array) => array,
            //Can't fail (shouldn't)
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        //Already checked that it's at least > 1
        let ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(pattern))
            | ScalarValue::LargeUtf8(Some(pattern))
            | ScalarValue::Utf8View(Some(pattern)),
        ) = &args.args[1]
        else {
            return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
        };

        let ColumnarValue::Scalar(ScalarValue::Int64(Some(position))) = args.args[2] else {
            return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
        };
        let position = position as usize - 1;
        //TODO: errors + reasonable defaults + length
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(occurrence))) = args.args[3] else {
            return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
        };
        let occurrence = occurrence as usize - 1;

        let ColumnarValue::Scalar(ScalarValue::Int64(Some(option))) = args.args[4] else {
            return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
        };
        let option = match option {
            0 | 1 => option as usize,
            n => {
                return regexp_errors::UnsupportedArgValueSnafu {
                    got: n.to_string(),
                    expected: "0 or 1".to_string(),
                }
                .fail()?;
            }
        };

        let ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(regexp_parameters))
            | ScalarValue::LargeUtf8(Some(regexp_parameters))
            | ScalarValue::Utf8View(Some(regexp_parameters)),
        ) = &args.args[5]
        else {
            return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
        };

        let ColumnarValue::Scalar(ScalarValue::Int64(Some(group_num))) = args.args[6] else {
            return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?;
        };
        let group_num = group_num as usize;

        let mut result_array =
            Decimal128Builder::with_capacity(array.len()).with_precision_and_scale(38, 0)?;

        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let string_array: &StringArray = as_generic_string_array(array)?;
                let regex = pattern_to_regex(pattern, regexp_parameters)
                    .context(regexp_errors::UnsupportedRegexSnafu)?;
                regexp(string_array, &regex, position).for_each(|opt_iter| {
                    result_array.append_option(
                        opt_iter
                            .and_then(|mut cap_iter| {
                                cap_iter.nth(occurrence).and_then(|cap| {
                                    cap.get(group_num).map(|mat| match option {
                                        0 => (mat.start() + position) as i128 + 1,
                                        1 => (mat.end() + position) as i128 + 1,
                                        _ => unreachable!(),
                                    })
                                })
                            })
                            .or(Some(0i128)),
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
}
