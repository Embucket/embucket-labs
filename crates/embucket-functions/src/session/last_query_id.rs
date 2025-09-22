use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

/// Returns the ID of a specified query in the current session.
/// If no query is specified, the most recently-executed query is returned.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LastQueryId {
    signature: Signature,
}

impl Default for LastQueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl LastQueryId {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::VariadicAny,
                    TypeSignature::Nullary,
                ]),
                volatility: Volatility::Volatile,
            },
        }
    }
}

impl ScalarUDFImpl for LastQueryId {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "last_query_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

crate::macros::make_udf_function!(LastQueryId);
