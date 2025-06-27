use arrow_schema::DataType::Time64;
use arrow_schema::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r"
Returns the current UTC time.

The `current_time()` return value is determined at query time and will return the same time, no matter when in the query plan the function executes.
",
    syntax_example = "current_time()"
)]
#[derive(Debug)]
pub struct CurrentTimeFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for CurrentTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentTimeFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::Numeric(1)],
                Volatility::Stable,
            ),
            aliases: vec!["localtime".to_string()],
        }
    }
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for CurrentTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "current_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        //TODO: support different return type precision based on the provided number
        // this only works with `ALTER SESSION SET TIME_OUTPUT_FORMAT = 'HH24:MI:SS.FF';`
        Ok(Time64(Microsecond))
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified current_time() function")
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    #[allow(clippy::unwrap_used)]
    fn simplify(&self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        let now_ts = info
            .execution_props()
            .query_execution_start_time
            .timestamp_micros()
            % 86_400_000_000;
        // let time = i32::try_from(now_ts % 86_400).ok();
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Time64Microsecond(Some(now_ts)),
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

crate::macros::make_udf_function!(CurrentTimeFunc);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::SimplifyContext;

    #[test]
    fn test_current_time() {
        let props = ExecutionProps::new();
        let now_ts = props.query_execution_start_time.timestamp_micros();
        let context = SimplifyContext::new(&props);
        let result = CurrentTimeFunc::new().simplify(vec![], &context);
        match result {
            Ok(ExprSimplifyResult::Simplified(Expr::Literal(ScalarValue::Time64Microsecond(
                Some(time),
            )))) => {
                assert_eq!(time, now_ts % 86_400_000_000);
            }
            _ => panic!("unexpected result"),
        }
    }
}
