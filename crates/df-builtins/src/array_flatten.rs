use crate::array_to_boolean;
use crate::booland::is_true;
use datafusion::arrow::array::{BooleanBuilder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::array::StringBuilder;
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ArrayFlattenFunc {
    signature: Signature,
}

impl Default for ArrayFlattenFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFlattenFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayFlattenFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let mut res = StringBuilder::with_capacity(arr.len(), 1024);

                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                for v in arr {
                    if let Some(v) = v {
                        res.append_option(flatten(v)?);
                    } else {
                        res.append_null();
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(res.finish())))
            }
            ColumnarValue::Scalar(v) => {
                if let ScalarValue::Utf8(v) = v {
                    if let Some(v) = v {
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(flatten(v)?)))
                    } else {
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                    }
                } else {
                    internal_err!("array_flatten function requires a string")
                }
            }
        }
    }
}

fn flatten(v: &str) -> DFResult<Option<String>> {
    Ok(Some("df".to_string()))
}

super::macros::make_udf_function!(ArrayFlattenFunc);

#[cfg(test)]

mod tests {
    use datafusion::arrow::util::pretty::print_batches;
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ArrayFlattenFunc::new()));
        let q = "SELECT array_flatten('[ [ [1, 2], [3] ], [ [4], [5] ] ]') as v;";
        let result = ctx.sql(q).await?.collect().await?;
        
        print_batches(&result)?;
        Ok(())
    }
}