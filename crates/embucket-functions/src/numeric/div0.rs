use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::sync::Arc;

/// `DIV0` SQL function
///
/// Performs division like the division operator (/), but returns 0 when the divisor is 0 (rather than reporting an error).
///
/// Syntax: `DIV0( <dividend> , <divisor> )`
///
/// Arguments:
/// - `dividend`: The number to be divided.
/// - `divisor`: The number by which to divide.
///
/// Example: `SELECT DIV0(10, 0) AS value;`
///
/// Returns:
/// - Returns the result of the division if the divisor is not zero.
/// - Returns 0 if the divisor is zero.
#[derive(Debug)]
pub struct Div0Func {
    signature: Signature,
}

impl Default for Div0Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Div0Func {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Support all numeric types
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Div0Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "div0"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Always return Float64 for consistency
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "DIV0 expects 2 arguments, got {}",
                args.len()
            )));
        }

        // Convert both arguments to arrays
        let dividend = args[0].clone().into_array(number_rows)?;
        let divisor = args[1].clone().into_array(number_rows)?;

        // Convert both arrays to Float64 for consistent division
        let dividend_f64 = cast_to_f64(&dividend)?;
        let divisor_f64 = cast_to_f64(&divisor)?;

        // Perform the division
        let result = div0_impl(&dividend_f64, &divisor_f64)?;

        // If both inputs were scalar, return a scalar
        if dividend.len() == 1 && divisor.len() == 1 {
            let scalar = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn cast_to_f64(array: &ArrayRef) -> DFResult<ArrayRef> {
    // Use DataFusion's cast functionality to convert any numeric type to Float64
    datafusion::arrow::compute::cast(array, &DataType::Float64)
        .map_err(|e| DataFusionError::Internal(format!("Failed to cast to Float64: {e}")))
}

fn div0_impl(dividend: &ArrayRef, divisor: &ArrayRef) -> DFResult<ArrayRef> {
    let dividend = dividend
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("Expected Float64Array for dividend".to_string())
        })?;

    let divisor = divisor
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("Expected Float64Array for divisor".to_string())
        })?;

    let result = (0..dividend.len())
        .map(|i| {
            if dividend.is_null(i) || divisor.is_null(i) {
                None
            } else {
                let div_val = divisor.value(i);
                if div_val == 0.0 {
                    Some(0.0)
                } else {
                    Some(dividend.value(i) / div_val)
                }
            }
        })
        .collect::<Float64Array>();

    Ok(Arc::new(result))
}

crate::macros::make_udf_function!(Div0Func);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_div0_nulls() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new()));

        let q = "SELECT DIV0(NULL, 2) AS null_dividend, 
                       DIV0(10, NULL) AS null_divisor, 
                       DIV0(NULL, NULL) AS both_null";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------+--------------+-----------+",
                "| null_dividend | null_divisor | both_null |",
                "+---------------+--------------+-----------+",
                "|               |              |           |",
                "+---------------+--------------+-----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_div0_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new()));

        let q = "SELECT DIV0(10, 2) AS normal_division, DIV0(10, 0) AS zero_division";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------+---------------+",
                "| normal_division | zero_division |",
                "+-----------------+---------------+",
                "| 5.0             | 0.0           |",
                "+-----------------+---------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_div0_numeric_types() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new()));

        let q = "SELECT DIV0(10, 2) AS int_int, 
                       DIV0(10.5, 2) AS float_int, 
                       DIV0(10, 2.5) AS int_float, 
                       DIV0(10.5, 2.5) AS float_float";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+-----------+-----------+-------------+",
                "| int_int | float_int | int_float | float_float |",
                "+---------+-----------+-----------+-------------+",
                "| 5.0     | 5.25      | 4.0       | 4.2         |",
                "+---------+-----------+-----------+-------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_div0_table_input() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new()));

        // Create a test table
        ctx.sql("CREATE TABLE div0_test (a INT, b INT)")
            .await?
            .collect()
            .await?;
        ctx.sql(
            "INSERT INTO div0_test VALUES (10, 2), (10, 0), (NULL, 2), (10, NULL), (NULL, NULL)",
        )
        .await?
        .collect()
        .await?;

        let q = "SELECT a, b, DIV0(a, b) AS result FROM div0_test";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----+---+--------+",
                "| a  | b | result |",
                "+----+---+--------+",
                "| 10 | 2 | 5.0    |",
                "| 10 | 0 | 0.0    |",
                "|    | 2 |        |",
                "| 10 |   |        |",
                "|    |   |        |",
                "+----+---+--------+",
            ],
            &result
        );

        Ok(())
    }
}
