use crate::string_binary::errors;
use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_string_array;
use datafusion_expr::Expr;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use std::any::Any;
use std::sync::Arc;

/// `LOWER` SQL function
///
/// Returns the lower string or the value itself for numeric types.
///
/// Syntax: `LOWER( <expr> )`
///
/// Arguments:
/// - `expr`: A string expression or a numeric value.
///
/// Example: `SELECT LOWER('abc') AS str_length, LOWER(123) AS num_value;`
///
/// Returns:
/// - For strings: lower string.
/// - For numeric values: the value itself as a string.
/// - For NULL: NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LowerFunc {
    signature: Signature,
}

impl Default for LowerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LowerFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Accept any type as input
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LowerFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lower"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() != 1 {
            return errors::LowerExpectsOneArgumentSnafu { count: args.len() }.fail()?;
        }

        let arg = &args[0];
        let array = match arg {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let len = array.len();
        let mut builder = StringBuilder::with_capacity(len, len * 10);

        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let string_array = as_string_array(&array)?;
                for i in 0..len {
                    if string_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = string_array.value(i);
                        builder.append_value(s.to_lowercase());
                    }
                }

                let result = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                let string_array = datafusion::arrow::compute::cast(&array, &DataType::Utf8)
                    .map_err(|e| {
                        errors::FailedToCastToUtf8Snafu {
                            error: e.to_string(),
                        }
                        .build()
                    })?;

                Ok(ColumnarValue::Array(string_array))
            }
        }
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> DFResult<ExprSimplifyResult> {
        if args.len() == 1
            && let Expr::Literal(scalar, _) = &args[0]
        {
            if scalar.is_null() {
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::Null,
                    None,
                )));
            }
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Utf8(Some(scalar.to_string().to_lowercase())),
                None,
            )));
        }

        Ok(ExprSimplifyResult::Original(args))
    }
}

crate::macros::make_udf_function!(LowerFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::SimplifyContext;

    #[tokio::test]
    async fn test_lower_sql() -> DFResult<()> {
        let f = LowerFunc::new();

        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]).to_dfschema_ref()?;
        let props = ExecutionProps::new();
        let context = SimplifyContext::new(&props).with_schema(schema);
        let resp = f.simplify(
            vec![Expr::Literal(
                ScalarValue::Utf8(Some("ABC".to_string())),
                None,
            )],
            &context,
        )?;

        let ExprSimplifyResult::Simplified(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) = resp
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(v, "abc".to_string());

        let resp = f.simplify(
            vec![Expr::Literal(ScalarValue::Int64(Some(123)), None)],
            &context,
        )?;

        let ExprSimplifyResult::Simplified(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) = resp
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(v, "123".to_string());

        let resp = f.simplify(
            vec![Expr::Literal(ScalarValue::Int64(None), None)],
            &context,
        )?;

        let ExprSimplifyResult::Simplified(Expr::Literal(ScalarValue::Null, _)) = resp else {
            panic!("Expected simplified expression");
        };

        assert_eq!(v, "123".to_string());
        Ok(())
    }
}
