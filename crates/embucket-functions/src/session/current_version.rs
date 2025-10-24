use crate::session::session_prop;
use crate::session_params::SessionParams;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// Returns the current Embucket version.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CurrentVersion {
    signature: Signature,
    session_params: Arc<SessionParams>,
}

impl Default for CurrentVersion {
    fn default() -> Self {
        Self::new(Arc::new(SessionParams::default()))
    }
}

impl CurrentVersion {
    #[must_use]
    pub fn new(session_params: Arc<SessionParams>) -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
            session_params,
        }
    }

    #[must_use]
    pub fn current_version(&self) -> String {
        self.session_params
            .get_property(&session_prop("current_version"))
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
    }
}

impl ScalarUDFImpl for CurrentVersion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "current_version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let value = self.current_version();
        let array = Arc::new(StringArray::from(vec![Some(value.as_str()); num_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

crate::macros::make_udf_function!(CurrentVersion);
