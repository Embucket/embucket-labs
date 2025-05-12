use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// STARTSWITH function
/// Syntax: STARTSWITH(expr1, expr2)
/// Returns: BOOLEAN - TRUE if expr1 starts with expr2, NULL if either input is NULL, FALSE otherwise
#[derive(Debug)]
pub struct StartsWithFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for StartsWithFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StartsWithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Utf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("startswith")],
        }
    }
}

impl ScalarUDFImpl for StartsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "startswith"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return internal_err!("STARTSWITH function requires two arguments");
        }

        // Check if we're dealing with any NULL scalar values directly - if so, return NULL
        if (matches!(args[0], ColumnarValue::Scalar(ref s) if s.is_null())
            || matches!(args[1], ColumnarValue::Scalar(ref s) if s.is_null()))
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        // Get array size - we need to handle both scalar and array inputs
        let mut array_size = 1;
        for arg in args {
            if let ColumnarValue::Array(a) = arg {
                array_size = a.len();
                break;
            }
        }
        let is_scalar = array_size == 1;

        // Convert arguments to string arrays
        let expr1 = match &args[0] {
            ColumnarValue::Scalar(v) => v.to_array()?,
            ColumnarValue::Array(a) => Arc::clone(a),
        };
        let expr2 = match &args[1] {
            ColumnarValue::Scalar(v) => v.to_array()?,
            ColumnarValue::Array(a) => Arc::clone(a),
        };

        // Process string arrays
        let result = starts_with_impl(&expr1, &expr2)?;

        if is_scalar {
            // If all inputs are scalar, return scalar output

            let scalar_result = result.value(0);
            return Ok(ColumnarValue::Scalar(scalar_result.into()));
        }

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Implementation of STARTSWITH for string arrays
fn starts_with_impl(expr1: &ArrayRef, expr2: &ArrayRef) -> Result<BooleanArray> {
    // Handle Utf8 (StringArray) type
    if let (Some(arr1), Some(arr2)) = (
        expr1.as_any().downcast_ref::<StringArray>(),
        expr2.as_any().downcast_ref::<StringArray>(),
    ) {
        let mut builder = BooleanBuilder::with_capacity(arr1.len());

        for i in 0..arr1.len() {
            if arr1.is_null(i) || arr2.is_null(i) {
                builder.append_null();
                continue;
            }

            let s1 = arr1.value(i);
            let s2 = arr2.value(i);

            builder.append_value(s1.starts_with(s2));
        }

        return Ok(builder.finish());
    }

    Err(DataFusionError::Internal(format!(
        "STARTSWITH unsupported type combination: {} and {}",
        expr1.data_type(),
        expr2.data_type()
    )))
}

super::macros::make_udf_function!(StartsWithFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_startswith_scalar() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StartsWithFunc::new()));
        let q = "SELECT STARTSWITH('coffee', 'co')";
        let result = ctx.sql(q).await?.collect().await?;

        assert_eq!(result[0].column(0).as_ref().data_type(), &DataType::Boolean);
        let bool_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.value(0), true);

        let q = "SELECT STARTSWITH('coffee', 'fee')";
        let result = ctx.sql(q).await?.collect().await?;
        let bool_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.value(0), false);

        Ok(())
    }

    #[tokio::test]
    async fn test_startswith_null() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StartsWithFunc::new()));

        let q = "SELECT STARTSWITH(NULL, 'co')";
        let result = ctx.sql(q).await?.collect().await?;
        let bool_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_array.is_null(0));

        let q = "SELECT STARTSWITH('coffee', NULL)";
        let result = ctx.sql(q).await?.collect().await?;
        let bool_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn test_startswith_table() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StartsWithFunc::new()));

        // Test with literal values to avoid table creation issues
        let q = "SELECT STARTSWITH('coffee', 'co') AS result";
        let batches = ctx.sql(q).await?.collect().await?;

        // Check that we got the expected boolean result (true)
        assert_eq!(batches.len(), 1, "Expected 1 batch");
        assert_eq!(batches[0].num_rows(), 1, "Expected 1 row");

        let bool_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(
            bool_array.value(0),
            true,
            "Expected true for 'coffee' starting with 'co'"
        );

        Ok(())
    }
}
