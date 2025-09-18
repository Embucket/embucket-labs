use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

// This is actually a stub, as it's implementation is by `cancel_query` function in `session_context.rs`
// of core-executor crate.

/// `SystemCancelQuery` is a scalar UDF named `SYSTEM$CANCEL_QUERY`, which cancels running query by query id (UUID).
///
/// # Purpose
/// This UDF has been added as a primary way to cancel queries, in a Snowflake compatible way.
///
/// # Arguments
/// - `query_id`: The string containing UUID of the query to cancel.
///
/// # SQL Usage
/// ```sql
/// SELECT SYSTEM$CANCEL_QUERY('123e4567-e89b-12d3-a456-426614174000');
/// ```
///
/// Cancel running query.
/// A user can cancel their own running SQL operations using this SQL function.
///
/// # Notes
/// - The function is marked as `Volatile`, indicating that it has side effects
///   (i.e., returns different results depending on target query is running or not)
///   and cannot be optimized away by the query planner.
#[derive(Debug)]
pub struct SystemCancelQuery {
    signature: Signature,
}

impl Default for SystemCancelQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemCancelQuery {
    #[must_use]
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

impl ScalarUDFImpl for SystemCancelQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "system$cancel_query"
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

crate::macros::make_udf_function!(SystemCancelQuery);
