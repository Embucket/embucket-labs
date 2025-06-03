use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::array::{BooleanArray, BooleanBuilder};
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::exec_err;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::error::Result as DFResult;

#[derive(Debug)]
pub struct ToArrayFunc {
    signature: Signature,
}

impl Default for ToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToArrayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "equal_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let lhs = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let rhs = match args[1].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        if lhs.len() != rhs.len() {
            return exec_err!("the first and second arguments must be of the same length");
        }

        let cmp_res = match apply_cmp(&args[0], &args[1], eq)? {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };
        let cmp_res = cmp_res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let mut res = BooleanBuilder::with_capacity(cmp_res.len());
        for (i, v) in cmp_res.iter().enumerate() {
            if let Some(v) = v {
                res.append_value(v);
            } else if lhs.is_null(i) && rhs.is_null(i) {
                res.append_value(true);
            } else {
                res.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(res.finish())))
    }
}

super::macros::make_udf_function!(EqualNullFunc);