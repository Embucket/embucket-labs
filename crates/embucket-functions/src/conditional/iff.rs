use crate::array_to_boolean;
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

// This function returns one of two values based on whether a given Boolean condition is true or false.
// It works like a basic if-then-else statement and is simpler than a CASE expression, since it only checks one condition.
// You can use it to apply conditional logic within SQL queries.
// Syntax: IFF( <condition> , <true_value> , <false_value> )
// Note: `iff` returns
// - This function is capable of returning a value of any data type. If the expression being returned has a value of NULL, then the function will also return NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IffFunc {
    signature: Signature,
}

impl Default for IffFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IffFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IffFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "iff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(return_type(&arg_types[1], &arg_types[2]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [input, lhs, rhs] = take_function_args(self.name(), args.args)?;
        let input = match &input {
            ColumnarValue::Scalar(v) => &v.to_array()?,
            ColumnarValue::Array(arr) => arr,
        };
        let input = array_to_boolean(input)?;
        let input_len = input.len();
        let lhs_dt = lhs.data_type();
        let rhs_dt = rhs.data_type();
        if lhs_dt != rhs_dt && lhs_dt != DataType::Null && rhs_dt != DataType::Null {
            return exec_err!(
                "IFF function requires the second and third arguments to be of the same type or NULL"
            );
        }
        let target_type = return_type(&lhs_dt, &rhs_dt);

        let lhs_array = match lhs {
            ColumnarValue::Scalar(scalar) => {
                scalar.cast_to(&target_type)?.to_array_of_size(input_len)?
            }
            ColumnarValue::Array(array) => cast(&array, &target_type)?,
        };

        let rhs_array = match rhs {
            ColumnarValue::Scalar(scalar) => {
                scalar.cast_to(&target_type)?.to_array_of_size(input_len)?
            }
            ColumnarValue::Array(array) => cast(&array, &target_type)?,
        };

        let result = zip(&input, &lhs_array, &rhs_array)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        Ok(ColumnarValue::Array(result))
    }
}

fn return_type(lhs: &DataType, rhs: &DataType) -> DataType {
    match (lhs, rhs) {
        (&DataType::Null, _) => rhs.clone(),
        (_, &DataType::Null) => lhs.clone(),
        _ => rhs.clone(),
    }
}

crate::macros::make_udf_function!(IffFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IffFunc::new()));
        let q = "SELECT IFF(TRUE, 'true', 'false') as f";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+------+", "| f    |", "+------+", "| true |", "+------+",],
            &result
        );

        let q = "SELECT IFF(FALSE, 'true', 'false') as f";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| f     |",
                "+-------+",
                "| false |",
                "+-------+",
            ],
            &result
        );

        let q = "SELECT IFF(NULL, 'true', 'false') as f";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| f     |",
                "+-------+",
                "| false |",
                "+-------+",
            ],
            &result
        );

        let q = "SELECT IFF(TRUE, NULL, 'false') as f";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| f |", "+---+", "|   |", "+---+",], &result);

        let q = "SELECT IFF(FALSE, 'true', NULL) as f";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| f |", "+---+", "|   |", "+---+",], &result);
        Ok(())
    }
}
