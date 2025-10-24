use crate::session::session_prop;
use crate::session_params::SessionParams;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// Returns the name of the current schema, which varies depending on where you call the function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CurrentSchema {
    signature: Signature,
    session_params: Arc<SessionParams>,
}

impl Default for CurrentSchema {
    fn default() -> Self {
        Self::new(Arc::new(SessionParams::default()))
    }
}

impl CurrentSchema {
    #[must_use]
    pub fn new(session_params: Arc<SessionParams>) -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
            session_params,
        }
    }

    #[must_use]
    pub fn current_schema(&self) -> String {
        self.session_params
            .get_property(&session_prop("current_schema"))
            .unwrap_or_else(|| "embucket".to_string())
    }
}

impl ScalarUDFImpl for CurrentSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let value = self.current_schema();
        let array = Arc::new(StringArray::from(vec![Some(value.as_str()); num_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

crate::macros::make_udf_function!(CurrentSchema);
