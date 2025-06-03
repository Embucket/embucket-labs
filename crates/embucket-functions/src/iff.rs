use crate::array_to_boolean;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

// Iff SQL function
// This function returns one of two values based on whether a given Boolean condition is true or false.
// It works like a basic if-then-else statement and is simpler than a CASE expression, since it only checks one condition.
// You can use it to apply conditional logic within SQL queries.
// Syntax: IFF( <condition> , <true_value> , <false_value> )
// Note: `iff` returns
// - This function is capable of returning a value of any data type. If the expression being returned has a value of NULL, then the function will also return NULL.
#[derive(Debug)]
pub struct IffFunc {
    signature: Signature,
}

impl Default for IffFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IffFunc {
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
        let arg1_dt = args.args[1].data_type();
        let arg2_dt = args.args[2].data_type();
        if arg1_dt != arg2_dt && arg1_dt != DataType::Null && arg2_dt != DataType::Null {
            return exec_err!(
                "Iff function requires the second and third arguments to be of the same type or NULL"
            );
        }

        let input = match &args.args[0] {
            ColumnarValue::Scalar(v) => &v.to_array()?,
            ColumnarValue::Array(arr) => arr,
        };

        let input = array_to_boolean(input)?;

        let lhs = match &args.args[1] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(_) => {
                return exec_err!("Iff function requires the second argument to be a scalar");
            }
        };

        let rhs = match &args.args[2] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(_) => {
                return exec_err!("Iff function requires the third argument to be a scalar");
            }
        };

        let lhs = lhs.cast_to(&return_type(&arg1_dt, &arg2_dt))?;
        let rhs = rhs.cast_to(&return_type(&arg1_dt, &arg2_dt))?;

        let mut res = Vec::with_capacity(input.len());
        for v in &input {
            if let Some(v) = v {
                if v {
                    res.push(lhs.clone());
                } else {
                    res.push(rhs.clone());
                }
            } else {
                res.push(rhs.clone());
            }
        }

        let arr = ScalarValue::iter_to_array(res)?;

        Ok(ColumnarValue::Array(arr))
    }
}

fn return_type(lhs: &DataType, rhs: &DataType) -> DataType {
    match (lhs, rhs) {
        (&DataType::Null, _) => rhs.clone(),
        (_, &DataType::Null) => lhs.clone(),
        _ => rhs.clone(),
    }
}

super::macros::make_udf_function!(IffFunc);

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
