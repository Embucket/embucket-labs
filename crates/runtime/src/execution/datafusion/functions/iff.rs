use crate::execution::datafusion::functions::array_to_boolean;
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, BooleanArray};
use arrow_schema::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct IffFunc {
    signature: Signature,
}

impl Default for IffFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IffFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IffFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "iff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args[1].data_type() != args.args[2].data_type() {
            return exec_err!(
                "Iff function requires the second and third arguments to be of the same type"
            );
        }

        let input = match &args.args[0] {
            ColumnarValue::Scalar(v) => &v.to_array()?,
            ColumnarValue::Array(arr) => arr,
        };

        let input = array_to_boolean(input)?;

        let lhs = match &args.args[1] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        let rhs = match &args.args[2] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        let mut res = vec![];
        for v in &input {
            if let Some(v) = v {
                if v {
                    res.push(lhs.clone());
                } else {
                    res.push(rhs.clone());
                }
            } else {
                res.push(rhs.clone())
            }
        }

        let arr = ScalarValue::iter_to_array(res)?;

        Ok(ColumnarValue::Array(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::print_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IffFunc::new()));
        let q = "select IFF(NULL, 'TRUE', 'FALSE') ";
        let result = ctx.sql(q).await?.collect().await?;

        print_batches(&result)?;

        Ok(())
    }
}
