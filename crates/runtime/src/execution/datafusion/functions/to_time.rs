use arrow_schema::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature};
use std::any::Any;

#[derive(Debug)]
pub struct ToTimeFunc {
    signature: Signature,
    aliases: Vec<String>,
    try_: bool,
}

impl Default for ToTimeFunc {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ToTimeFunc {
    pub fn new(try_: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Numeric(1),
                    TypeSignature::String(1),
                    TypeSignature::Exact(vec![DataType::Boolean]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["time".to_string()],
            try_,
        }
    }
}

impl ScalarUDFImpl for ToTimeFunc {
    fn as_any(&self) -> &dyn Any {
        &self
    }

    fn name(&self) -> &str {
        if self.try_ {
            "try_to_time"
        } else {
            "to_time"
        }
    }

    fn aliases(&self) -> &[String] {
        &["time".to_string()]
    }
    
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Time64(TimeUnit::Nanosecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        todo!()
    }
}
