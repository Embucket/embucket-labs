use datafusion::arrow::{array::UInt64Array, datatypes::DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Coercion, TypeSignature, TypeSignatureClass};
use datafusion_common::cast::as_string_array;
use datafusion_common::types::logical_string;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
use strsim::jaro_winkler;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JarowinklerSimilarityFunc {
    signature: Signature,
}

impl Default for JarowinklerSimilarityFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl JarowinklerSimilarityFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Coercible(vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JarowinklerSimilarityFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jarowinkler_similarity"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::as_conversions
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let left_arr = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let right_arr = match &args[1] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let left = as_string_array(left_arr)?;
        let right = as_string_array(right_arr)?;

        let result = crate::macros::izip!(left.iter(), right.iter())
            .map(|(l, r)| {
                if let (Some(lv), Some(rv)) = (l, r) {
                    if lv.is_empty() || rv.is_empty() {
                        return Some(0);
                    }
                    let lv = lv.to_lowercase();
                    let rv = rv.to_lowercase();
                    let sim = jaro_winkler(&lv, &rv);
                    Some((sim * 100.0).floor() as u64)
                } else {
                    None
                }
            })
            .collect::<UInt64Array>();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

crate::macros::make_udf_function!(JarowinklerSimilarityFunc);
