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
pub struct Div0 {
    signature: Signature,
}

impl Default for Div0 {
    fn default() -> Self {
        Self::new()
    }
}

impl Div0 {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Support all numeric types
            signature: Signature::numeric(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Div0 {
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

crate::macros::make_udf_function!(Div0);
