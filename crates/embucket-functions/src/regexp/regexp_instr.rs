use super::errors as regexp_errors;
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
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use snafu::ResultExt;
use crate::utils::{pattern_to_regex, regexp};

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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        //Already checked that it's at least > 1
        let subject = &args.args[0];
        let array = match subject {
            ColumnarValue::Array(array) => array,
            //Can't fail (shouldn't)
            ColumnarValue::Scalar(scalar) => &scalar.to_array()?,
        };

        //Already checked that it's at least > 1
        let pattern = &args.args[1];
        let pattern = match pattern {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(str))
                | ScalarValue::LargeUtf8(Some(str))
                | ScalarValue::Utf8View(Some(str)),
            ) => str,
            _ => return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?,
        };

        let position = &args.args[2];
        let position = match position {
            ColumnarValue::Scalar(
                ScalarValue::UInt64(Some(int))
            ) => *int,
            _ => return regexp_errors::FormatMustBeNonNullScalarValueSnafu.fail()?,
        };

        let mut result_array =
            Decimal128Builder::with_capacity(array.len()).with_precision_and_scale(38, 0)?;
        
        let occurence = 0;

        let group_num = 0;
        
        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let string_array: &StringArray = as_generic_string_array(array)?;
                let regex = pattern_to_regex(pattern).context(regexp_errors::UnsupportedRegexSnafu)?;
                regexp(string_array, &regex, position as usize).for_each(|opt_iter| {
                    result_array.append_option(opt_iter.and_then(|mut cap_iter| {
                        cap_iter.nth(occurence).and_then(|cap| {
                            cap.get(group_num).map(|mat| mat.start() as i128)
                        })
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
}
