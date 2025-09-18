use crate::numeric::errors;
use datafusion::arrow::datatypes::DECIMAL128_MAX_PRECISION;
use datafusion::arrow::array::{Array, ArrowNativeTypeOp, Decimal128Array, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use rust_decimal::Decimal;
use snafu::ResultExt;
use std::any::Any;
use std::cmp::{max, min};
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
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Div0Func {
    signature: Signature,
    null: bool,
}

impl Default for Div0Func {
    fn default() -> Self {
        Self::new(false)
    }
}

impl Div0Func {
    #[must_use]
    pub fn new(null: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            null,
        }
    }
}

impl ScalarUDFImpl for Div0Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.null { "div0null" } else { "div0" }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(if arg_types[0].is_null() || arg_types[1].is_null() {
            if self.null && arg_types[1].is_null() {
                let (p, s) =
                    calculate_precision_and_scale(&arg_types[0], &DataType::Decimal128(38, 0));
                DataType::Decimal128(p, s)
            } else {
                DataType::Null
            }
        } else if arg_types[0].is_floating() || arg_types[1].is_floating() {
            DataType::Float64
        } else {
            let (p, s) = calculate_precision_and_scale(&arg_types[0], &arg_types[1]);
            DataType::Decimal128(p, s)
        })
    }

    #[allow(
        clippy::cast_lossless,
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::too_many_lines
    )]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        let dividend = args[0].clone().into_array(number_rows)?;
        let divisor = args[1].clone().into_array(number_rows)?;

        if (dividend.data_type().is_null() || divisor.data_type().is_null()) && !self.null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        if dividend.data_type().is_floating() || divisor.data_type().is_floating() {
            let dividend = if dividend.data_type().is_floating() {
                dividend
            } else {
                datafusion::arrow::compute::cast(&dividend, &DataType::Float64).context(
                    errors::CastToTypeSnafu {
                        target_type: "Float64",
                    },
                )?
            };

            let divisor = if divisor.data_type().is_floating() {
                divisor
            } else {
                datafusion::arrow::compute::cast(&divisor, &DataType::Float64).context(
                    errors::CastToTypeSnafu {
                        target_type: "Float64",
                    },
                )?
            };

            let divided = dividend
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    errors::UnexpectedArrayTypeSnafu {
                        expected: "Float64Array",
                        actual: format!("{:?}", dividend.data_type()),
                    }
                    .build()
                })?;

            let divisor = divisor
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    errors::UnexpectedArrayTypeSnafu {
                        expected: "Float64Array",
                        actual: format!("{:?}", divisor.data_type()),
                    }
                    .build()
                })?;

            let result = divided
                .into_iter()
                .zip(divisor)
                .map(|(divided, divisor)| match (divided, divisor) {
                    (Some(dividend), Some(divisor)) => {
                        if divisor.is_zero() {
                            Some(0.0)
                        } else {
                            Some(dividend / divisor)
                        }
                    }
                    _ => {
                        if self.null {
                            Some(0.0)
                        } else {
                            None
                        }
                    }
                })
                .collect::<Vec<_>>();

            let rb = Float64Array::from(result);

            Ok(ColumnarValue::Array(Arc::new(rb)))
        } else {
            let dividend = {
                if let DataType::Decimal128(_, _) = dividend.data_type() {
                    dividend
                } else {
                    datafusion::arrow::compute::cast(
                        &dividend,
                        &DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                    )
                    .context(errors::CastToTypeSnafu {
                        target_type: "Decimal128",
                    })?
                }
            };

            let dividend = dividend
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    errors::UnexpectedArrayTypeSnafu {
                        expected: "Decimal128Array",
                        actual: format!("{:?}", dividend.data_type()),
                    }
                    .build()
                })?;

            let divisor = {
                if let DataType::Decimal128(_, _) = divisor.data_type() {
                    divisor
                } else {
                    datafusion::arrow::compute::cast(
                        &divisor,
                        &DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                    )
                    .context(errors::CastToTypeSnafu {
                        target_type: "Decimal128",
                    })?
                }
            };

            let divisor = divisor
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    errors::UnexpectedArrayTypeSnafu {
                        expected: "Decimal128Array",
                        actual: format!("{:?}", divisor.data_type()),
                    }
                    .build()
                })?;

            let (p, s) = calculate_precision_and_scale(dividend.data_type(), divisor.data_type());
            let dividend_scale = dividend.scale();
            let divisor_scale = divisor.scale();

            let a = dividend
                .into_iter()
                .zip(divisor)
                .map(|(dividend, divisor)| match (dividend, divisor) {
                    (Some(dividend), Some(divisor)) => {
                        // todo fallback to bigdecimal precision>29
                        let dividend =
                            Decimal::from_i128_with_scale(dividend, dividend_scale as u32);
                        let divisor = Decimal::from_i128_with_scale(divisor, divisor_scale as u32);

                        if divisor.is_zero() {
                            Some(0)
                        } else {
                            let mut r = dividend / divisor;
                            r.rescale(s as u32);
                            Some(r.mantissa())
                        }
                    }
                    _ => {
                        if self.null {
                            Some(0)
                        } else {
                            None
                        }
                    }
                })
                .collect::<Vec<_>>();

            let rb = Decimal128Array::from(a).with_precision_and_scale(p, s)?;
            Ok(ColumnarValue::Array(Arc::new(rb)))
        }
    }
}

#[allow(
    clippy::cast_lossless,
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
fn calculate_precision_and_scale(dividend: &DataType, divisor: &DataType) -> (u8, i8) {
    let (p1, s1) = match dividend {
        DataType::Decimal128(p, s) => (*p, *s as u8),
        _ => (38, 0),
    };
    let s2 = match divisor {
        DataType::Decimal128(_, s) => *s as u8,
        _ => 0,
    };

    let l1 = p1.saturating_sub(s1);
    let l_output = l1.saturating_add(s2);
    let s_output = max(s1, min(s1.saturating_add(6), 12));
    let p_output_unclamped = l_output.saturating_add(s_output);
    let snowflake_max_precision: u8 = 38;
    let final_p = min(p_output_unclamped, snowflake_max_precision);

    (final_p, s_output as i8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_float() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

        let q = "SELECT DIV0(1, 0.1) as a, DIV0(0.1,1) as b, DIV0(0.1, 0.1) as c";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------+-----+-----+",
                "| a    | b   | c   |",
                "+------+-----+-----+",
                "| 10.0 | 0.1 | 1.0 |",
                "+------+-----+-----+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_decimal() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

        let q = "SELECT DIV0(1.0::DECIMAL(5,3), 1) as a, DIV0(0.1::decimal,1) as b, DIV0(0.1::decimal, 10000) as c";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------+----------------+----------------+",
                "| a           | b              | c              |",
                "+-------------+----------------+----------------+",
                "| 1.000000000 | 0.100000000000 | 0.000010000000 |",
                "+-------------+----------------+----------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_nulls() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

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
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

        let q = "SELECT DIV0(10, 2) AS normal_division, DIV0(10, 0) AS zero_division";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------+---------------+",
                "| normal_division | zero_division |",
                "+-----------------+---------------+",
                "| 5.000000        | 0.000000      |",
                "+-----------------+---------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_numeric_types() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

        let q = "SELECT DIV0(10, 2) AS int_int, 
                       DIV0(10.5, 2) AS float_int, 
                       DIV0(10, 2.5) AS int_float, 
                       DIV0(10.5, 2.5) AS float_float";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+-----------+-----------+-------------+",
                "| int_int  | float_int | int_float | float_float |",
                "+----------+-----------+-----------+-------------+",
                "| 5.000000 | 5.25      | 4.0       | 4.2         |",
                "+----------+-----------+-----------+-------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_negative_types() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

        let q = "SELECT DIV0(-10, 2) AS int_int, 
                       DIV0(10.5, -2) AS float_int, 
                       DIV0(-10, -2.5) AS int_float, 
                       DIV0(-10.5, -2.5) AS float_float";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+-----------+-----------+-------------+",
                "| int_int   | float_int | int_float | float_float |",
                "+-----------+-----------+-----------+-------------+",
                "| -5.000000 | -5.25     | 4.0       | 4.2         |",
                "+-----------+-----------+-----------+-------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_table_input() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(false)));

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
                "+----+---+----------+",
                "| a  | b | result   |",
                "+----+---+----------+",
                "| 10 | 2 | 5.000000 |",
                "| 10 | 0 | 0.000000 |",
                "|    | 2 |          |",
                "| 10 |   |          |",
                "|    |   |          |",
                "+----+---+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_div0null() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Div0Func::new(true)));

        let q = "SELECT DIV0NULL(10, 0) AS a, DIV0NULL(10, NULL) AS b,  DIV0NULL(10::DECIMAL(20,10), 0) AS c";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----------+----------+----------------+",
                "| a        | b        | c              |",
                "+----------+----------+----------------+",
                "| 0.000000 | 0.000000 | 0.000000000000 |",
                "+----------+----------+----------------+",
            ],
            &result
        );

        Ok(())
    }
}
